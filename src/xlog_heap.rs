use std::ffi::CStr;

use pgrx::{
    pg_sys::{self, ItemPointerSetInvalid},
    PgBox,
};

use crate::{
    decoder::DecodedResult, relation::get_relid_from_rlocator, xlog_reader::get_block_tag,
};

fn item_pointer_set_invalid(mut item_pointer: pg_sys::ItemPointerData) {
    assert!(item_pointer.ip_posid != 0);
    item_pointer.ip_blkid.bi_hi = 0xFFFF;
    item_pointer.ip_blkid.bi_lo = 0xFFFF;
    item_pointer.ip_posid = pg_sys::InvalidOffsetNumber;
}

pub fn get_heap_tuple(
    xlog_reader: &PgBox<pg_sys::XLogReaderState>,
    record: &PgBox<pg_sys::DecodedXLogRecord>,
    page: pg_sys::Page,
    relid: pg_sys::Oid,
    old: bool,
) -> Option<pg_sys::HeapTuple> {
    let main_data = record.main_data;
    let offnum = match u32::from(record.header.xl_info) & (pg_sys::XLOG_HEAP_OPMASK) {
        pg_sys::XLOG_HEAP_INSERT => {
            if old {
                // No previous row version available
                return None;
            }
            let xlrec = unsafe { PgBox::from_pg(main_data.cast::<pg_sys::xl_heap_insert>()) };
            xlrec.offnum
        }
        pg_sys::XLOG_HEAP_DELETE => {
            if !old {
                // No next row version available
                return None;
            }
            let xlrec = unsafe { PgBox::from_pg(main_data.cast::<pg_sys::xl_heap_delete>()) };
            xlrec.offnum
        }
        pg_sys::XLOG_HEAP_HOT_UPDATE | pg_sys::XLOG_HEAP_UPDATE => {
            let xlrec = unsafe { PgBox::from_pg(main_data.cast::<pg_sys::xl_heap_update>()) };
            if old {
                xlrec.old_offnum
            } else {
                xlrec.new_offnum
            }
        }
        _ => return None,
    };

    unsafe {
        let item_id = pg_sys::PageGetItemId(page, offnum);
        let htup_len = PgBox::from_pg(item_id).lp_len();
        let htuple = pg_sys::PageGetItem(page, item_id);
        // Create the fake tuple
        let mut tuple = PgBox::<pg_sys::HeapTupleData>::alloc0();
        tuple.t_data = htuple.cast();
        tuple.t_len = htup_len;
        item_pointer_set_invalid(tuple.t_self);
        tuple.t_tableOid = relid;
        Some(tuple.into_pg())
    }
}

pub fn decode_heap_record(
    xlog_reader: &PgBox<pg_sys::XLogReaderState>,
    record: &PgBox<pg_sys::DecodedXLogRecord>,
) -> Option<DecodedResult> {
    if record.max_block_id < 0 {
        // No need to process anything if there's no blocks
        return None;
    }
    let heap_op = u32::from(record.header.xl_info) & pg_sys::XLOG_HEAP_OPMASK;
    let op_name = unsafe { pg_sys::heap_identify(heap_op.try_into().unwrap()) };
    let op_name_str = unsafe { CStr::from_ptr(op_name).to_str().unwrap() };
    pg_sys::info!(
        "Processing HEAP record {} at LSN {}",
        op_name_str,
        xlog_reader.ReadRecPtr
    );

    let (rlocator, _, _) = get_block_tag(xlog_reader);
    let Some(relid) = get_relid_from_rlocator(&rlocator) else {
        pg_sys::warning!("Couldn't find oid for rlocator {:?}", rlocator);
        return None;
    };

    //    match heap_op {
    //        XLOG_HEAP_INSERT => ,
    //    }

    Some(DecodedResult {
        lsn: record.lsn.cast_signed(),
        dboid: rlocator.dbOid,
        relid,
        xid: record.header.xl_xid,
        redo_query: None,
        revert_query: None,
        row_before: None,
        row_after: None,
    })
}
