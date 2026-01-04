use std::ffi::CStr;

use pgrx::iter::TableIterator;
use pgrx::pg_sys::InvalidXLogRecPtr;
use pgrx::spi::Error;
use pgrx::{
    error,
    ffi::c_char,
    pg_sys::{self, RmgrIds::RM_HEAP_ID, XLogRecord},
    PgBox,
};
use pgrx::{name, PgMemoryContexts};

use crate::pg_lsn::PgLSN;
use crate::xlog_heap::decode_heap_record;
use crate::XLogReaderPrivate;
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

/// Advance xlogreader to the next record
pub fn read_next_record(
    xlog_reader: &PgBox<pg_sys::XLogReaderState>,
) -> Result<Option<*mut XLogRecord>, WalError> {
    let mut errormsg: *mut c_char = std::ptr::null_mut();
    let record = unsafe { pg_sys::XLogReadRecord(xlog_reader.as_ptr(), &raw mut errormsg) };
    if record.is_null() {
        let private =
            unsafe { PgBox::from_pg(xlog_reader.private_data.cast::<XLogReaderPrivate>()) };
        if private.endptr_reached {
            return Ok(None);
        }
        if !errormsg.is_null() {
            let msg = unsafe { CStr::from_ptr(errormsg).to_string_lossy().into_owned() };
            return Err(WalError::ReadRecordError(xlog_reader.EndRecPtr, msg));
        }
    }
    Ok(Some(record))
}

/// Process all WAL records until limit, endptr or end of wal is reached
pub fn decode_wal_records(
    xlog_reader: &PgBox<pg_sys::XLogReaderState>,
    startptr: PgLSN,
) -> (Vec<DecodedResult>, Option<WalError>) {
    let mut mem_ctx = PgMemoryContexts::new("Per record");
    let mut res = vec![];

    let first_record = unsafe { pg_sys::XLogFindNextRecord(xlog_reader.as_ptr(), startptr.into()) };
    if first_record == u64::from(InvalidXLogRecPtr) {
        error!("could not find a valid record after {}", startptr);
    }

    loop {
        // Move to the next record
        if let Err(e) = read_next_record(xlog_reader) {
            return (res, Some(e));
        }
        // Get the latest decoded record from xlog reader
        let record = unsafe { PgBox::from_pg(xlog_reader.record) };
        // Switch to per record memory context
        let mut old_ctx = unsafe { mem_ctx.set_as_current() };

        let rmid = u32::from(record.header.xl_rmid);
        let decoded_record = match rmid {
            RM_HEAP_ID => decode_heap_record(xlog_reader, &record),
            _default => None,
        };

        decoded_record.map(|d| res.push(d));

        // Clean up
        unsafe { old_ctx.set_as_current() };
        unsafe { mem_ctx.reset() };
        pg_sys::check_for_interrupts!();
    }
}
