use std::ffi::CStr;

use pgrx::{
    error,
    ffi::c_char,
    pg_sys::{self, XLogRecord},
    PgBox,
};

use crate::XLogReaderPrivate;

pub fn read_next_record(state: *mut pg_sys::XLogReaderState) -> Option<*mut XLogRecord> {
    let pg_state = unsafe { PgBox::from_pg(state) };
    let mut errormsg: *mut c_char = std::ptr::null_mut();
    let record = unsafe { pg_sys::XLogReadRecord(state, &raw mut errormsg) };

    if record.is_null() {
        let private = unsafe { PgBox::from_pg((*state).private_data.cast::<XLogReaderPrivate>()) };
        if private.endptr_reached {
            return None;
        }
        if !errormsg.is_null() {
            let msg = unsafe { CStr::from_ptr(errormsg).to_string_lossy() };
            error!(
                "Could not read WAL at {}/{}: {}",
                pg_state.EndRecPtr >> 32,
                (pg_state.EndRecPtr & 0xffff) as u32,
                msg
            );
        }
    }
    Some(record)
}
