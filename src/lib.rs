mod pg_lsn;
mod decoder;
mod relation;
mod tuple_str;
mod wal;
mod xlog_heap;
mod xlog_reader;

use std::{
    ffi::{c_void, CStr, CString},
    fmt::Display,
    fs::File,
    io,
    os::fd::AsRawFd,
    path::Path,
};

use pgrx::{
    pg_sys::{TimeLineID, WALRead, XLogReaderState, XLogSegNo, XLOG_BLCKSZ},
    prelude::*,
};

use crate::{
    pg_lsn::{PgLSN, xlog_file_name}, decoder::WalDecoder, wal::detect_wal_dir
};

::pgrx::pg_module_magic!(name, version);

#[allow(clippy::type_complexity)]
#[pg_extern]
fn pg_waldecoder(
    start_lsn: &str,
    end_lsn: default!(Option<&str>, "NULL"),
    timeline: default!(i32, 1),
    wal_dir: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(lsn, i64),
        name!(dboid, pg_sys::Oid),
        name!(relid, pg_sys::Oid),
        name!(xid, pg_sys::TransactionId),
        name!(redo_query, &'static str),
        name!(revert_query, &'static str),
        name!(row_before, &'static str),
        name!(row_after, &'static str),
    ),
> {
    info!("Called with: {start_lsn:?}, {end_lsn:?}, {timeline:?}, {wal_dir:?}");

    // Parse start ptr
    let startptr = match PgLSN::try_from(start_lsn) {
        Ok(startptr) => startptr,
        Err(e) => error!("Error: {}", e.to_string()),
    };

    let wal_decoder = WalDecoder::new(startptr, end_lsn, timeline, wal_dir);
//    let (results, err) = decode_wal_records(&xlog_reader, startptr);
    TableIterator::new(wal_decoder)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::{pg_lsn::PgLSN, decoder::{DecodedResult, WalDecoder}};
    use pgrx::{pg_sys::XLogRecPtr, prelude::*};
    use std::ffi::{CStr, CString};

    #[pg_test]
    fn test_pg_waldecoder() {
        let startptr = unsafe { PgLSN::from(pg_sys::GetXLogWriteRecPtr()) };

        unsafe {
            Spi::run("CREATE TABLE test AS SELECT generate_series(1, 100) as id, '' AS data");
            // Transaction isn't committed yet, force a flush so we can read the records from the
            // WAL
            pg_sys::XLogFlush(pg_sys::XactLastRecEnd);
        }
        let wal_decoder = WalDecoder::new(startptr, None, 1, None);
        let results = wal_decoder.take(4).collect::<Vec<DecodedResult>>();
        assert_eq!(results.len(), 4);
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
