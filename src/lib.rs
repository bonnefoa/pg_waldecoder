#![allow(dead_code)]
mod lsn_utils;
mod wal_utils;

use std::ffi::{c_void, CString};
use thiserror::Error;

use pgrx::{pg_sys::{InvalidXLogRecPtr, XLOG_BLCKSZ}, prelude::*};

use crate::lsn_utils::lsn_to_rec_ptr;

::pgrx::pg_module_magic!(name, version);

struct XLogReaderPrivate {
    timeline: u32,
    start_ptr: u64,
    end_ptr: u64,
    endptr_reached: bool,
}

#[pg_extern]
fn pg_waldecoder_path(wal_path: &str) -> TableIterator<
    'static,
    (
        name!(oid, i64),
        name!(relid, i64),
        name!(xid, pg_sys::TransactionId),
        name!(redo_query, &'static str),
        name!(revert_query, &'static str),
        name!(row_before, &'static str),
        name!(row_after, &'static str),
    ),>
{
    todo!()
}

#[pg_extern]
fn pg_waldecoder(
    start_lsn: &str,
    end_lsn: default!(&str, "NULL"),
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

    // Parse arguments
    let start_ptr = match lsn_to_rec_ptr(start_lsn) {
        Ok(start_ptr) => start_ptr,
        Err(e) => error!("Error: {}", e.to_string()),
    };

    let end_ptr = match lsn_to_rec_ptr(end_lsn) {
        Ok(end_ptr) => end_ptr,
        Err(e) => error!("Error: {}", e.to_string()),
    };

    let private_data = Box::new(XLogReaderPrivate {
        timeline: timeline.cast_unsigned(),
        start_ptr,
        end_ptr,
        endptr_reached: false,
    });
    let xl_routine = Box::new(pg_sys::XLogReaderRoutine {
        page_read: Some(pg_waldecoder_read_page),
        segment_open: None,
        segment_close: None,
    });

    let wal_dir_ptr = match wal_dir {
        None => std::ptr::null(),
        Some(d) => CString::new(d).unwrap().as_c_str().as_ptr(),
    };
    let xlog_reader = unsafe {
        pg_sys::XLogReaderAllocate(
            123,
            wal_dir_ptr,
            Box::into_raw(xl_routine),
            Box::into_raw(private_data).cast::<c_void>(),
        )
    };

    let first_record = unsafe { pg_sys::XLogFindNextRecord(xlog_reader, start_ptr) };
    if first_record == u64::from(InvalidXLogRecPtr) {
        error!(
            "could not find a valid record after {:X}/{:X}",
            start_ptr >> 32,
            (start_ptr & 0xff00) as u32
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

#[pg_guard]
unsafe extern "C-unwind" fn pg_waldecoder_read_page(
    state: *mut pg_sys::XLogReaderState,
    target_page_ptr: pg_sys::XLogRecPtr,
    req_len: i32,
    target_ptr: pg_sys::XLogRecPtr,
    read_buff: *mut i8,
) -> i32 {
    let private = unsafe { PgBox::from_pg((*state).private_data) };
    let count = XLOG_BLCKSZ;

    todo!()
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
        let _res = crate::pg_waldecoder("0/01800028", "0/01800D28", 1, Some(wal_dir));
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
