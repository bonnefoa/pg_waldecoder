#![allow(dead_code)]
mod lsn;
mod wal;
mod record;

use std::{
    ffi::{CStr, CString, c_void}, fs::File, io, os::fd::AsRawFd, path::Path
};

use pgrx::{
    pg_sys::{InvalidXLogRecPtr, TimeLineID, WALRead, WALReadError, XLogSegNo, XLOG_BLCKSZ},
    prelude::*,
};

use crate::{
    lsn::{lsn_to_rec_ptr, xlog_file_name},
    wal::detect_wal_dir,
};

::pgrx::pg_module_magic!(name, version);

struct XLogReaderPrivate {
    timeline: u32,
    startptr: u64,
    endptr: Option<u64>,
    endptr_reached: bool,
    opened_segment: Option<File>,
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_waldecoder_read_page(
    state: *mut pg_sys::XLogReaderState,
    target_page_ptr: pg_sys::XLogRecPtr,
    req_len: i32,
    _target_ptr: pg_sys::XLogRecPtr,
    read_buff: *mut i8,
) -> i32 {
    let pg_state = unsafe { PgBox::from_pg(state) };
    let mut private = unsafe { PgBox::from_pg((*state).private_data.cast::<XLogReaderPrivate>()) };
    let blcksz = u64::from(XLOG_BLCKSZ);
    let count = match private.endptr {
        Some(endptr) => {
            if target_page_ptr + blcksz <= endptr {
                blcksz
            } else if target_page_ptr + u64::from(req_len.cast_unsigned()) <= endptr {
                endptr - target_page_ptr
            } else {
                private.endptr_reached = true;
                return -1;
            }
        }
        None => blcksz,
    };

    let errinfo = Box::into_raw(Box::new(WALReadError::default()));
    if !WALRead(
        state,
        read_buff,
        target_page_ptr,
        usize::try_from(count).unwrap(),
        private.timeline,
        errinfo,
    ) {
        let errinfo = Box::from_raw(errinfo);
        let seg = errinfo.wre_seg;
        let fname = xlog_file_name(seg.ws_tli, seg.ws_segno, pg_state.segcxt.ws_segsize);

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
    next_seg_no: XLogSegNo,
    tli_ptr: *mut TimeLineID,
) {
    let mut pg_state = unsafe { PgBox::from_pg(state) };
    let mut private = unsafe { PgBox::from_pg((*state).private_data.cast::<XLogReaderPrivate>()) };
    let fname = xlog_file_name(*tli_ptr, next_seg_no, pg_state.segcxt.ws_segsize);
    let wal_dir = CStr::from_ptr(pg_state.segcxt.ws_dir.as_ptr())
        .to_str()
        .expect("Error converting wal_dir to cstr");
    let path = Path::new(wal_dir).join(&fname);
    let Ok(f) = File::open(&path) else {
        error!("Could not open file \"{}\"", fname);
    };
    info!("Opening segment {}", path.display());
    pg_state.seg.ws_file = f.as_raw_fd();
    private.opened_segment = Some(f);
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_waldecoder_segment_close(
    state: *mut pg_sys::XLogReaderState,
) {
    let mut private = unsafe { PgBox::from_pg((*state).private_data.cast::<XLogReaderPrivate>()) };
    private.opened_segment = None;
}

// #[pg_extern]
// fn pg_waldecoder_path(
//     wal_path: &str,
// ) -> TableIterator<
//     'static,
//     (
//         name!(oid, i64),
//         name!(relid, i64),
//         name!(xid, pg_sys::TransactionId),
//         name!(redo_query, &'static str),
//         name!(revert_query, &'static str),
//         name!(row_before, &'static str),
//         name!(row_after, &'static str),
//     ),
// > {
//     todo!()
// }

#[pg_extern]
fn pg_waldecoder(
    start_lsn: &str,
    end_lsn: default!(Option<&str>, "NULL"),
    timeline: default!(i32, 1),
    wal_dir: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(oid, i64),
        name!(relid, i64),
        name!(xid, pg_sys::TransactionId),
        name!(redo_query, &'static str),
        name!(revert_query, &'static str),
        name!(row_before, &'static str),
        name!(row_after, &'static str),
    ),
> {
    info!("Called with: {start_lsn:?}, {end_lsn:?}, {timeline:?}, {wal_dir:?}");

    // Parse start ptr
    let startptr = match lsn_to_rec_ptr(start_lsn) {
        Ok(startptr) => startptr,
        Err(e) => error!("Error: {}", e.to_string()),
    };

    // Parse end ptr
    let endptr = match end_lsn.map(lsn_to_rec_ptr) {
        Some(Ok(endptr)) => Some(endptr),
        Some(Err(e)) => error!("Error: {}", e.to_string()),
        None => None,
    };

    let private_data = Box::new(XLogReaderPrivate {
        timeline: timeline.cast_unsigned(),
        startptr,
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

    let first_record = unsafe { pg_sys::XLogFindNextRecord(xlog_reader, startptr) };
    if first_record == u64::from(InvalidXLogRecPtr) {
        error!(
            "could not find a valid record after {:X}/{:X}",
            startptr >> 32,
            (startptr & 0xff00) as u32
        );
    }

    let results = vec![(
        1,
        1,
        pg_sys::TransactionId::from(1),
        "redo_query",
        "revert_query",
        "row before",
        "row_after",
    )];

    TableIterator::new(results)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    macro_rules! test_case {
        ($dirname:expr) => {
            concat!(env!("CARGO_MANIFEST_DIR"), "/resources/test/", $dirname)
        };
    }

    #[pg_test]
    fn test_pg_waldecoder() {
        let wal_dir = test_case!("18_single_upgrade");
        // let wal_file = test_case!("000000010000000000000018");
        let _res = crate::pg_waldecoder("0/01800028", Some("0/01800D28"), 1, Some(wal_dir));
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
