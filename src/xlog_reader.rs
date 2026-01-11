use std::collections::HashMap;
use std::ffi::{c_void, CStr, CString};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

use pgrx::iter::TableIterator;
use pgrx::pg_sys::{DecodedBkpBlock, InvalidXLogRecPtr, Oid};
use pgrx::spi::Error;
use pgrx::{
    error,
    ffi::c_char,
    pg_sys::{self, RmgrIds::RM_HEAP_ID, XLogRecord},
    PgBox,
};
use pgrx::{info, name, pg_guard, warning, PgMemoryContexts};

use crate::pg_lsn::{xlog_file_name, PgLSN};
use crate::record::get_block;
use crate::relation::get_relid_from_rlocator;
use crate::wal::detect_wal_dir;
use thiserror::Error;

use pgrx::pg_sys::{RelFileLocator, XLogRecGetBlockTag};

/// Get block tag info from latest decoded record
pub fn get_block_tag(xlog_reader: &PgBox<pg_sys::XLogReaderState>) -> (RelFileLocator, i32, u32) {
    let mut rlocator: RelFileLocator = RelFileLocator {
        spcOid: 0.into(),
        dbOid: 0.into(),
        relNumber: 0.into(),
    };
    let mut forknum: i32 = 0;
    let mut blknum: u32 = 0;
    unsafe {
        XLogRecGetBlockTag(
            xlog_reader.as_ptr(),
            0,
            &raw mut rlocator,
            &raw mut forknum,
            &raw mut blknum,
        );
    };
    (rlocator, forknum, blknum)
}

pub struct XLogReaderPrivate {
    pub timeline: u32,
    pub endptr: Option<PgLSN>,
    pub endptr_reached: bool,
    pub opened_segment: Option<File>,
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

pub fn new(
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
