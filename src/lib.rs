mod record;
mod decoder;
mod pg_lsn;
mod relation;
mod tuple_str;
mod wal;
mod xlog_heap;
mod xlog_reader;


use pgrx::prelude::*;

use crate::{
    decoder::WalDecoder,
    pg_lsn::PgLSN,
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
        name!(redo_query, Option<&'static str>),
        name!(revert_query, Option<&'static str>),
        name!(row_before, Option<&'static str>),
        name!(row_after, Option<&'static str>),
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
    TableIterator::new(wal_decoder.map(std::convert::Into::into))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::{
        decoder::{DecodedResult, WalDecoder},
        pg_lsn::PgLSN,
    };
    use pgrx::prelude::*;

    #[pg_test]
    fn test_decode_fpw() {
        unsafe {
            Spi::run("CREATE TABLE test (id int, data text);");
            Spi::run("Insert INTO test (id) values (1)");
            Spi::run("CHECKPOINT;");
            pg_sys::XLogFlush(pg_sys::XactLastRecEnd);
        }

        let startptr = unsafe { PgLSN::from(pg_sys::GetXLogWriteRecPtr()) };
        unsafe {
            Spi::run("Insert INTO test (id) values (2)");
            // Transaction isn't committed yet, force a flush so we can read the records from the
            // WAL
            pg_sys::XLogFlush(pg_sys::XactLastRecEnd);
        }

        let wal_decoder = WalDecoder::new(startptr, None, 1, None);
        let results = wal_decoder.take(4).collect::<Vec<DecodedResult>>();
        assert_eq!(results.len(), 1);
        let decoded_record = &results[0];
        assert!(decoded_record.redo_query.is_some());
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
