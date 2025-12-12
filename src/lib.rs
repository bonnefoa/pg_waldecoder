use std::{
    ffi::{c_void, CString},
    path,
};
use thiserror::Error;

use pgrx::{pg_sys::InvalidXLogRecPtr, prelude::*};

::pgrx::pg_module_magic!(name, version);

#[derive(Clone, Debug, Hash, Ord, PartialOrd, PartialEq, Eq, Error)]
pub enum InvalidLSN {
    #[error("No LSN provided")]
    NoLSN,
    #[error("Invalid filename")]
    InvalidFileName,
    #[error("Invalid hex value in '{0}': `{1}`")]
    InvalidHexValue(String, String),
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
        Err(e) => return Err(InvalidLSN::InvalidHexValue(lsn.to_string(), e.to_string())),
    };

    let xrecoff_str = iter.next().unwrap();
    let xrecoff = match u64::from_str_radix(xrecoff_str, 16) {
        Ok(xrecoff) => xrecoff,
        Err(e) => return Err(InvalidLSN::InvalidHexValue(lsn.to_string(), e.to_string())),
    };
    Ok(xlogid << 32 | xrecoff)
}

fn filename_to_startptr(filename: Option<&str>, wal_segsz_bytes: i32) -> Result<u64, InvalidLSN> {
    let filename = match filename {
        Some(filename) => filename,
        None => return Err(InvalidLSN::NoLSN),
    };
    let filename = match path::Path::new(filename)
        .file_name()
        .and_then(|s| s.to_str())
    {
        Some(p) => p,
        None => return Err(InvalidLSN::InvalidFileName),
    };

    // let _tli = match u64::from_str_radix(&filename[0..8], 16) {
    //     Ok(tli) => tli,
    //     Err(e) => {
    //         return Err(InvalidLSN::InvalidHexValue(
    //             filename[0..8].to_string(),
    //             e.to_string(),
    //         ))
    //     }
    // };

    let log_str = &filename[8..16];
    let log = match u64::from_str_radix(log_str, 16) {
        Ok(log) => log,
        Err(e) => {
            return Err(InvalidLSN::InvalidHexValue(
                log_str.to_string(),
                e.to_string(),
            ))
        }
    };

    let seg_str = &filename[16..24];
    let seg = match u64::from_str_radix(seg_str, 16) {
        Ok(seg) => seg,
        Err(e) => {
            return Err(InvalidLSN::InvalidHexValue(
                seg_str.to_string(),
                e.to_string(),
            ))
        }
    };
    Ok(log * 0x100000000 * (wal_segsz_bytes as u64) + seg)
}

#[pg_extern]
fn pg_waldecoder(
    start_lsn: default!(Option<&str>, "NULL"),
    _end_lsn: default!(Option<&str>, "NULL"),
    wal_dir: default!(Option<&str>, "NULL"),
    wal_file: default!(Option<&str>, "NULL"),
    wal_sg_size: default!(Option<i32>, 16777216),
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
    info!("Called with: {start_lsn:?}, {_end_lsn:?}, {wal_dir:?}, {wal_file:?}, {wal_sg_size:?}");
    let wal_sg_size = wal_sg_size.unwrap_or(16777216);
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
            wal_sg_size,
            wal_dir_ptr,
            Box::into_raw(xl_routine),
            Box::into_raw(private_data) as *mut c_void,
        )
    };

    let start_ptr = match lsn_to_startptr(start_lsn).or(filename_to_startptr(wal_file, wal_sg_size))
    {
        Ok(start_ptr) => start_ptr,
        Err(e) => error!("Error: {}", e.to_string()),
    };

    let first_record = unsafe { pg_sys::XLogFindNextRecord(xlog_reader, start_ptr) };
    if first_record == (InvalidXLogRecPtr as u64) {
        error!(
            "could not find a valid record after {:X}/{:X}",
            start_ptr >> 32,
            start_ptr as u32
        );
    };

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
        let wal_file = test_case!("000000010000000000000018");
        let _res = crate::pg_waldecoder(None, None, Some(wal_dir), Some(wal_file), None);
    }

    #[test]
    fn test_lsn_to_startptr() {
        let res = crate::lsn_to_startptr(Some("0/01800C50"));
        assert_eq!(res.unwrap(), 25168976);
        let res = crate::lsn_to_startptr(Some("2/01800C50"));
        assert_eq!(res.unwrap(), 8615103568);
    }

    #[test]
    fn test_filename_to_startptr() {
        let res = crate::filename_to_startptr(Some("000000010000000000000018"), 1048576);
        assert_eq!(res.unwrap(), 24);
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
