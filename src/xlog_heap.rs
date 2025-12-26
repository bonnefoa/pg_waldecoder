use std::ffi::CStr;

use pgrx::{
    info,
    pg_sys::{self, Oid},
    PgBox,
};

use crate::{lsn::format_lsn, relation::get_relid_from_rlocator, xlog_reader::get_block_tag};

pub fn decode_heap_record(
    xlog_reader: &PgBox<pg_sys::XLogReaderState>,
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
        format_lsn(xlog_reader.ReadRecPtr)
    );

    let (rlocator, _, _) = get_block_tag(xlog_reader);
    let relid = get_relid_from_rlocator(&rlocator);
    info!("Got relid {relid:?}");
}
