use std::{ffi::CStr, mem::MaybeUninit};

use pgrx::{
    info,
    pg_sys::{self, Oid},
    PgBox,
};

use crate::lsn::format_lsn;

fn get_relid_from_record(rlocator: &pg_sys::RelFileLocator) -> Oid {
    todo!()
}

pub fn decode_heap_record(
    state: &PgBox<pg_sys::XLogReaderState>,
    record: &PgBox<pg_sys::DecodedXLogRecord>,
) {
    if record.max_block_id < 0 {
        // No need to process anything if there's no blocks
        return;
    }
    let heap_op = u32::from(record.header.xl_info) & pg_sys::XLOG_HEAP_OPMASK;
    let op_name = unsafe { pg_sys::heap_identify(heap_op.try_into().unwrap()) };
    let op_name_str = unsafe { CStr::from_ptr(op_name).to_str().unwrap() };
    info!(
        "Processing HEAP record {} at LSN {}",
        op_name_str,
        format_lsn(state.ReadRecPtr)
    );
}
