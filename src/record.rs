use std::ffi::CStr;

use pgrx::pg_sys::InvalidXLogRecPtr;
use pgrx::PgMemoryContexts;
use pgrx::{
    error,
    ffi::c_char,
    pg_sys::{self, RmgrIds::RM_HEAP_ID, XLogRecord},
    PgBox,
};

use crate::lsn::format_lsn;
use crate::xlog_heap::decode_heap_record;
use crate::XLogReaderPrivate;

/// Advance xlogreader to the next record
pub fn read_next_record(xlog_reader: *mut pg_sys::XLogReaderState) -> Option<*mut XLogRecord> {
    let pg_state = unsafe { PgBox::from_pg(xlog_reader) };
    let mut errormsg: *mut c_char = std::ptr::null_mut();
    let record = unsafe { pg_sys::XLogReadRecord(xlog_reader, &raw mut errormsg) };
    if record.is_null() {
        let private =
            unsafe { PgBox::from_pg((*xlog_reader).private_data.cast::<XLogReaderPrivate>()) };
        if private.endptr_reached {
            return None;
        }
        if !errormsg.is_null() {
            let msg = unsafe { CStr::from_ptr(errormsg).to_string_lossy() };
            error!(
                "Could not read WAL at {}: {}",
                format_lsn(pg_state.EndRecPtr),
                msg
            );
        }
    }
    Some(record)
}

pub fn decode_wal_records(xlog_reader: *mut pg_sys::XLogReaderState, startptr: u64) {
    let pg_state = unsafe { PgBox::from_pg(xlog_reader) };
    let mut mem_ctx = PgMemoryContexts::new("Per record");

    let first_record = unsafe { pg_sys::XLogFindNextRecord(xlog_reader, startptr) };
    if first_record == u64::from(InvalidXLogRecPtr) {
        error!(
            "could not find a valid record after {}",
            format_lsn(startptr)
        );
    }

    loop {
        // Move to the next record
        if read_next_record(xlog_reader).is_none() {
            return;
        }
        // Get the latest decoded record from xlog reader
        let record = unsafe { PgBox::from_pg(pg_state.record) };
        // Switch to per record memory context
        let mut old_ctx = unsafe { mem_ctx.set_as_current() };

        let rmid = u32::from(record.header.xl_rmid);
        match rmid {
            RM_HEAP_ID => decode_heap_record(&pg_state, &record),
            _default => (),
        }

        // Clean up
        unsafe { old_ctx.set_as_current() };
        unsafe { mem_ctx.reset() };
        pg_sys::check_for_interrupts!();
    }
}
