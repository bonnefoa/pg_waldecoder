use std::ffi::{c_void, CStr, CString};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

use pgrx::iter::TableIterator;
use pgrx::pg_sys::InvalidXLogRecPtr;
use pgrx::spi::Error;
use pgrx::{
    error,
    ffi::c_char,
    pg_sys::{self, RmgrIds::RM_HEAP_ID, XLogRecord},
    PgBox,
};
use pgrx::{info, name, pg_guard, warning, PgMemoryContexts};

use crate::pg_lsn::{xlog_file_name, PgLSN};
use crate::wal::detect_wal_dir;
use crate::xlog_heap::decode_heap_record;
use thiserror::Error;

#[derive(Clone, Debug, Hash, Ord, PartialOrd, PartialEq, Eq, Error)]
pub enum WalError {
    #[error("Could not read WAL at {0}: {1}")]
    ReadRecordError(pg_sys::XLogRecPtr, String),
}

pub type DecodedResult = (
    name!(lsn, i64),
    name!(dboid, pg_sys::Oid),
    name!(relid, pg_sys::Oid),
    name!(xid, pg_sys::TransactionId),
    name!(redo_query, &'static str),
    name!(revert_query, &'static str),
    name!(row_before, &'static str),
    name!(row_after, &'static str),
);

pub struct WalDecoder {
    xlog_reader: PgBox<pg_sys::XLogReaderState>,
    startptr: PgLSN,
    per_record_ctx: PgMemoryContexts,
}

struct XLogReaderPrivate {
    timeline: u32,
    endptr: Option<PgLSN>,
    endptr_reached: bool,
    opened_segment: Option<File>,
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_waldecoder_read_page(
    state: *mut pg_sys::XLogReaderState,
    target_page_ptr: u64,
    req_len: i32,
    target_ptr: u64,
    read_buff: *mut i8,
) -> i32 {
    let target_page_ptr = PgLSN::from(target_page_ptr);
    let target_ptr = PgLSN::from(target_ptr);
    info!("Reading page {}", target_page_ptr);
    let xlog_reader = unsafe { PgBox::from_pg(state) };
    let mut private = unsafe { PgBox::from_pg((*state).private_data.cast::<XLogReaderPrivate>()) };
    let blcksz = pg_sys::XLOG_BLCKSZ;
    let count = match private.endptr {
        Some(endptr) => {
            if target_page_ptr + blcksz <= endptr {
                blcksz
            } else if target_page_ptr + req_len <= endptr {
                (endptr - target_page_ptr).try_into().unwrap()
            } else {
                private.endptr_reached = true;
                return -1;
            }
        }
        None => blcksz,
    };

    let errinfo = Box::into_raw(Box::new(pg_sys::WALReadError::default()));
    if !pg_sys::WALRead(
        state,
        read_buff,
        target_page_ptr.into(),
        usize::try_from(count).unwrap(),
        private.timeline,
        errinfo,
    ) {
        let errinfo = Box::from_raw(errinfo);
        let seg = errinfo.wre_seg;
        let fname = xlog_file_name(seg.ws_tli, seg.ws_segno, xlog_reader.segcxt.ws_segsize);

        if errinfo.wre_errno != 0 {
            let error = io::Error::from_raw_os_error(errinfo.wre_errno);
            error!(
                "could not read from file {0}, offset {1}: {2}",
                fname, errinfo.wre_off, error
            );
        } else {
            error!(
                "could not read from file {0}, offset {1}: read {2} of {3}",
                fname, errinfo.wre_off, errinfo.wre_read, errinfo.wre_req
            );
        }
    }
    i32::try_from(count).unwrap()
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_waldecoder_segment_open(
    state: *mut pg_sys::XLogReaderState,
    next_seg_no: pg_sys::XLogSegNo,
    tli_ptr: *mut pg_sys::TimeLineID,
) {
    let mut xlog_reader = unsafe { PgBox::from_pg(state) };
    let mut private =
        unsafe { PgBox::from_pg(xlog_reader.private_data.cast::<XLogReaderPrivate>()) };
    let fname = xlog_file_name(*tli_ptr, next_seg_no, xlog_reader.segcxt.ws_segsize);
    let wal_dir = CStr::from_ptr(xlog_reader.segcxt.ws_dir.as_ptr())
        .to_str()
        .expect("Error converting wal_dir to cstr");
    let path = Path::new(wal_dir).join(&fname);
    let Ok(f) = File::open(&path) else {
        error!("Could not open file \"{}\"", path.display());
    };
    info!("Opening segment {}", path.display());
    xlog_reader.seg.ws_file = f.as_raw_fd();
    private.opened_segment = Some(f);
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_waldecoder_segment_close(state: *mut pg_sys::XLogReaderState) {
    let mut private = unsafe { PgBox::from_pg((*state).private_data.cast::<XLogReaderPrivate>()) };
    private.opened_segment = None;
}

fn build_xlog_reader(
    start_lsn: PgLSN,
    end_lsn: Option<&str>,
    timeline: i32,
    wal_dir: Option<&str>,
) -> PgBox<pg_sys::XLogReaderState> {
    // Parse end ptr
    let endptr = match end_lsn.map(PgLSN::try_from) {
        Some(Ok(endptr)) => Some(endptr),
        Some(Err(e)) => error!("Error: {}", e.to_string()),
        None => None,
    };

    let private_data = Box::new(XLogReaderPrivate {
        timeline: timeline.cast_unsigned(),
        endptr,
        endptr_reached: false,
        opened_segment: None,
    });

    let xl_routine = Box::new(pg_sys::XLogReaderRoutine {
        page_read: Some(pg_waldecoder_read_page),
        segment_open: Some(pg_waldecoder_segment_open),
        segment_close: Some(pg_waldecoder_segment_close),
    });

    let Some((wal_dir, segsz)) = detect_wal_dir(wal_dir) else {
        error!("No valid WAL files found in wal dir")
    };
    info!("Detected Wal dir: {}, segsz: {}", wal_dir.display(), segsz);

    let wal_dir_cstr = CString::new(wal_dir.to_str().expect("wal_dir conversion error"))
        .expect("WAL dir cstring conversion failed");
    let wal_dir_ptr = wal_dir_cstr.as_c_str().as_ptr();

    let xlog_reader = unsafe {
        pg_sys::XLogReaderAllocate(
            segsz.cast_signed(),
            wal_dir_ptr,
            Box::into_raw(xl_routine),
            Box::into_raw(private_data).cast::<c_void>(),
        )
    };
    unsafe { PgBox::from_pg(xlog_reader) }
}

impl Iterator for WalDecoder {
    type Item = DecodedResult;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Move to the next record
            let mut errormsg: *mut c_char = std::ptr::null_mut();
            let record =
                unsafe { pg_sys::XLogReadRecord(self.xlog_reader.as_ptr(), &raw mut errormsg) };
            if record.is_null() {
                let private = unsafe {
                    PgBox::from_pg(self.xlog_reader.private_data.cast::<XLogReaderPrivate>())
                };
                if private.endptr_reached {
                    return None;
                }
                if !errormsg.is_null() {
                    let msg = unsafe { CStr::from_ptr(errormsg).to_string_lossy().into_owned() };
                    warning!("Error getting next wal record: {msg}");
                    // return Err(WalError::ReadRecordError(self.xlog_reader.EndRecPtr, msg));
                    return None;
                }
            }

            // Get the latest decoded record from xlog reader
            let record = unsafe { PgBox::from_pg(self.xlog_reader.record) };
            let rmid = u32::from(record.header.xl_rmid);

            if rmid != RM_HEAP_ID {
                // Move to the next record
                continue;
            }

            // Switch to per record memory context
            let mut old_ctx = unsafe { self.per_record_ctx.set_as_current() };

            let decoded_record = match rmid {
                RM_HEAP_ID => decode_heap_record(&self.xlog_reader, &record),
                _ => panic!("Unexpected record type"),
            };

            // Clean up
            unsafe { old_ctx.set_as_current() };
            unsafe { self.per_record_ctx.reset() };
            pg_sys::check_for_interrupts!();

            return decoded_record;
        }
        None
    }
}

impl WalDecoder {
    pub fn new(
        startptr: PgLSN,
        end_lsn: Option<&str>,
        timeline: i32,
        wal_dir: Option<&str>,
    ) -> WalDecoder {
        // Build the xlog reader
        let xlog_reader = build_xlog_reader(startptr, end_lsn, timeline, wal_dir);
        let mut per_record_ctx = PgMemoryContexts::new("Per decoded record");

        // Check we have can find valid wal files
        let first_record =
            unsafe { pg_sys::XLogFindNextRecord(xlog_reader.as_ptr(), startptr.into()) };
        if first_record == u64::from(InvalidXLogRecPtr) {
            error!("could not find a valid record after {}", startptr);
        }

        WalDecoder {
            xlog_reader,
            startptr,
            per_record_ctx,
        }
    }
}
