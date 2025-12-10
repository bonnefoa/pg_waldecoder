use std::ffi::{c_void, CString};
use thiserror::Error;

use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

#[derive(Clone, Debug, Hash, Ord, PartialOrd, PartialEq, Eq, Error)]
pub enum InvalidLSN {
    #[error("No LSN provided")]
    NoLSN,
    #[error("Invalid hex value: `{0}`")]
    InvalidHexValue(String),
}

fn lsn_to_startptr(lsn: Option<&str>) -> Result<u64, InvalidLSN> {
    let lsn = match lsn {
        Some(lsn) => lsn,
        None => return Err(InvalidLSN::NoLSN),
    };

    let mut iter = lsn.split("/");
    let xlogid_str = iter.next().unwrap();
    let xlogid = match u64::from_str_radix(xlogid_str, 16) {
        Ok(xlogid) => xlogid,
        Err(e) => return Err(InvalidLSN::InvalidHexValue(e.to_string())),
    };

    let xrecoff_str = iter.next().unwrap();
    let xrecoff = match u64::from_str_radix(xrecoff_str, 16) {
        Ok(xrecoff) => xrecoff,
        Err(e) => return Err(InvalidLSN::InvalidHexValue(e.to_string())),
    };
    Ok(xlogid << 32 | xrecoff)
}

#[pg_extern]
fn pg_waldecoder(
    start_lsn: Option<&str>,
    _end_lsn: Option<i64>,
    wal_dir: Option<&str>,
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
    let wal_segment_size = 8;
    let private_data = Box::new(pg_sys::ReadLocalXLogPageNoWaitPrivate { end_of_wal: false });
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
            wal_segment_size,
            wal_dir_ptr,
            Box::into_raw(xl_routine),
            Box::into_raw(private_data) as *mut c_void,
        )
    };

    let start_ptr = match lsn_to_startptr(start_lsn) {
        Ok(start_ptr) => start_ptr,
        Err(err_msg) => error!("Invalid start ptr: {}", err_msg),
    };
    unsafe { pg_sys::XLogFindNextRecord(xlog_reader, start_ptr) };

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
    _state: *mut pg_sys::XLogReaderState,
    _target_page_ptr: pg_sys::XLogRecPtr,
    _req_len: i32,
    _target_ptr: pg_sys::XLogRecPtr,
    _read_buff: *mut i8,
) -> i32 {
    0
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
        let _res = crate::pg_waldecoder(None, None, Some(wal_dir));
        // assert_eq!("Hello, pg_waldecoder", );
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
