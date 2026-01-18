use std::collections::HashMap;
use std::ffi::CStr;
use std::fmt::{Display, Formatter};
use std::mem;

use pgrx::pg_sys::{DecodedBkpBlock, InvalidXLogRecPtr, Oid, PGAlignedBlock, RmgrIds};
use pgrx::prelude::PgHeapTuple;
use pgrx::{
    error,
    ffi::c_char,
    pg_sys::{self, RmgrIds::RM_HEAP_ID},
    PgBox,
};
use pgrx::{info, warning, AllocatedByRust, PgMemoryContexts, PgTupleDesc};

use crate::pg_lsn::PgLSN;
use crate::relation::get_relid_from_rlocator;
use crate::{item, page, tuple_str, xlog_reader};

pub struct DecodedResult {
    pub lsn: i64,
    pub dboid: pg_sys::Oid,
    pub relid: pg_sys::Oid,
    pub xid: pg_sys::TransactionId,
    pub redo_query: Option<String>,
    pub revert_query: Option<String>,
    pub row_before: Option<String>,
    pub row_after: Option<String>,
}

impl From<DecodedResult>
    for (
        i64,
        pg_sys::Oid,
        pg_sys::Oid,
        pg_sys::TransactionId,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )
{
    fn from(val: DecodedResult) -> Self {
        (
            val.lsn,
            val.dboid,
            val.relid,
            val.xid,
            val.redo_query,
            val.revert_query,
            val.row_before,
            val.row_after,
        )
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PageId {
    spc_oid: pg_sys::Oid,
    db_oid: pg_sys::Oid,
    rel_number: pg_sys::RelFileNumber,
    blknum: pg_sys::BlockNumber,
}

impl PageId {
    fn new(blk: &PgBox<pg_sys::DecodedBkpBlock>) -> PageId {
        PageId {
            spc_oid: blk.rlocator.spcOid,
            db_oid: blk.rlocator.dbOid,
            rel_number: blk.rlocator.relNumber,
            blknum: blk.blkno,
        }
    }
}

impl Display for PageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}, blk {}",
            self.spc_oid, self.db_oid, self.rel_number, self.blknum
        )
    }
}

pub struct WalDecoder {
    xlog_reader: PgBox<pg_sys::XLogReaderState>,
    /// Current record from `xlog_reader`
    record: PgBox<pg_sys::DecodedXLogRecord>,
    per_record_ctx: PgMemoryContexts,
    function_ctx: PgMemoryContexts,
    page_hash: HashMap<PageId, PgBox<pg_sys::PGAlignedBlock, AllocatedByRust>>,
}

impl Iterator for WalDecoder {
    type Item = DecodedResult;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.move_to_next_record() {
                return None;
            }

            // Box decoded record from xlog reader
            self.record = unsafe { PgBox::from_pg(self.xlog_reader.record) };

            info!(
                "Processing record at {}",
                PgLSN::from(self.xlog_reader.ReadRecPtr)
            );

            // Switch to per record memory context
            let mut old_ctx = unsafe { self.per_record_ctx.set_as_current() };

            let decoded_record = self.process_current_record();
            if decoded_record.is_none() {
                continue;
            }

            // Clean up
            unsafe { old_ctx.set_as_current() };
            unsafe { self.per_record_ctx.reset() };
            pg_sys::check_for_interrupts!();

            return decoded_record;
        }
        None
    }
}

fn get_block_data(blk: &PgBox<DecodedBkpBlock>) -> Option<*mut i8> {
    if !blk.in_use {
        return None;
    }
    if !blk.has_data {
        return None;
    }
    Some(blk.data)
}

const SIZE_OF_PAGE_HEADER_DATA: usize = mem::offset_of!(pg_sys::PageHeaderData, pd_linp);

const SIZE_OF_HEAP_HEADER: usize =
    mem::offset_of!(pg_sys::xl_heap_header, t_hoff) + mem::size_of::<u8>();
const SIZE_OF_HEAP_TUPLE_HEADER: usize =
    mem::offset_of!(pg_sys::HeapTupleHeaderData, t_bits) + mem::size_of::<u8>();
const SIZE_OF_HEAP_INSERT: usize =
    mem::offset_of!(pg_sys::xl_heap_insert, flags) + mem::size_of::<u8>();
const HEAP_TUPLE_SIZE: usize = mem::size_of::<pg_sys::HeapTupleData>();
const MAX_HEAP_TUPLE_SIZE: usize =
    pg_sys::BLCKSZ as usize - (SIZE_OF_PAGE_HEADER_DATA + mem::size_of::<pg_sys::ItemIdData>());

impl WalDecoder {
    /// Create a new `WalDecoder`
    pub fn new(
        startptr: PgLSN,
        end_lsn: Option<&str>,
        timeline: i32,
        wal_dir: Option<&str>,
    ) -> WalDecoder {
        // Build the xlog reader
        let xlog_reader = xlog_reader::new(end_lsn, timeline, wal_dir);
        let per_record_ctx = PgMemoryContexts::new("Per decoded record");
        let function_ctx = PgMemoryContexts::CurrentMemoryContext;

        // Check we have can find valid wal files
        let first_record =
            unsafe { pg_sys::XLogFindNextRecord(xlog_reader.as_ptr(), startptr.into()) };
        if first_record == u64::from(InvalidXLogRecPtr) {
            error!("could not find a valid record after {}", startptr);
        }

        let page_hash = HashMap::new();
        let record = PgBox::null();
        WalDecoder {
            xlog_reader,
            record,
            per_record_ctx,
            function_ctx,
            page_hash,
        }
    }

    /// Advance reader to the next record. Returns true if end is reached.
    fn move_to_next_record(&mut self) -> bool {
        let mut errormsg: *mut c_char = std::ptr::null_mut();
        let record =
            unsafe { pg_sys::XLogReadRecord(self.xlog_reader.as_ptr(), &raw mut errormsg) };
        if record.is_null() {
            let private = unsafe {
                PgBox::from_pg(
                    self.xlog_reader
                        .private_data
                        .cast::<xlog_reader::XLogReaderPrivate>(),
                )
            };
            if private.endptr_reached {
                return true;
            }
            if !errormsg.is_null() {
                let msg = unsafe { CStr::from_ptr(errormsg).to_string_lossy().into_owned() };
                warning!("Error getting next wal record: {msg}");
                return true;
            }
        }
        false
    }

    fn process_fpw(&mut self, blk_id: u8) {
        let blk = self.get_block(blk_id);
        let page_id = PageId::new(&blk);
        if !blk.has_image || !blk.apply_image {
            // No FPW to restore
            return;
        }

        // We have a FPW, create the page and insert it
        let page = unsafe {
            // Allocate the page
            let page = PgBox::<pg_sys::PGAlignedBlock>::alloc0_in_context(
                self.per_record_ctx.parent().unwrap(),
            );
            let ok = pg_sys::RestoreBlockImage(
                self.xlog_reader.as_ptr(),
                blk_id,
                page.as_ptr().cast::<i8>(),
            );
            if !ok {
                pg_sys::error!(
                    "{}",
                    CStr::from_ptr(self.xlog_reader.errormsg_buf)
                        .to_str()
                        .unwrap()
                );
            }
            page
        };
        self.page_hash.insert(page_id.clone(), page);
    }

    fn get_rmid(&self) -> u32 {
        u32::from(self.record.header.xl_rmid)
    }

    fn get_page(&self, page_id: &PageId) -> Option<PgBox<pg_sys::PageHeaderData>> {
        self.page_hash
            .get(page_id)
            .map(|page| unsafe { PgBox::from_pg(page.as_ptr().cast::<pg_sys::PageHeaderData>()) })
    }

    fn process_init_record(&mut self, blk_id: u8) {
        let blk = self.get_block(blk_id);
        let page_id = PageId::new(&blk);

        if (self.record.header.xl_info & u8::try_from(pg_sys::XLOG_HEAP_INIT_PAGE).unwrap()) == 0 {
            return;
        }
        info!("Found a init heap for {page_id}, insert page in hashmap");
        let page = unsafe {
            PgBox::<pg_sys::PGAlignedBlock>::alloc0_in_context(
                self.per_record_ctx.parent().unwrap(),
            )
        };
        unsafe {
            pg_sys::PageInit(
                page.as_ptr().cast(),
                mem::size_of::<pg_sys::PGAlignedBlock>(),
                0,
            );
        };
        let page_cast = unsafe { PgBox::from_pg(page.as_ptr().cast::<pg_sys::PageHeaderData>()) };
        self.page_hash.insert(page_id.clone(), page);
    }

    /// Get block at index `blk_id` for the current record
    fn get_block(&mut self, blk_id: u8) -> PgBox<pg_sys::DecodedBkpBlock> {
        unsafe { PgBox::from_pg(self.record.blocks.as_mut_ptr().add(blk_id as usize)) }
    }

    fn process_current_record(&mut self) -> Option<DecodedResult> {
        let rmid = self.get_rmid();
        if rmid != RM_HEAP_ID {
            // TODO: Handle xlog, xact and heap2 records
            info!("rmid {rmid}, skipping");
            return None;
        }

        if self.record.max_block_id < 0 {
            // No blocks available, skip it
            warning!("No blocks available, skipping record");
            return None;
        }

        // TODO: Iterate through all blocks and apply fpw

        let blk_id = 0;

        // Do we have a FPW to apply?
        self.process_fpw(blk_id);
        // Or a init record?
        self.process_init_record(blk_id);

        let blk = self.get_block(blk_id);
        let page_id = PageId::new(&blk);
        let Some(page) = self.get_page(&page_id) else {
            warning!("No page found for {page_id}, skipping record");
            return None;
        };
        pg_sys::info!("Page {:?}", page);

        let Some(relid) = get_relid_from_rlocator(&blk.rlocator) else {
            pg_sys::warning!("Couldn't find oid for rlocator {:?}", blk.rlocator);
            return None;
        };

        match rmid {
            RM_HEAP_ID => self.decode_heap_record(&page, &blk, relid),
            _ => panic!("Unsupported record type"),
        }
    }

    pub fn decode_heap_record(
        &self,
        page: &PgBox<pg_sys::PageHeaderData>,
        blk: &PgBox<DecodedBkpBlock>,
        relid: Oid,
    ) -> Option<DecodedResult> {
        let heap_op = self.get_heap_op();
        let op_name = unsafe { pg_sys::heap_identify(heap_op.try_into().unwrap()) };
        let op_name_str = unsafe { CStr::from_ptr(op_name).to_str().unwrap() };
        pg_sys::info!(
            "Processing HEAP record {} at LSN {}",
            op_name_str,
            self.xlog_reader.ReadRecPtr
        );

        let (relname, tupdesc) = unsafe {
            let relation = PgBox::from_pg(pg_sys::table_open(
                relid,
                pg_sys::AccessShareLock.cast_signed(),
            ));
            // let tupdesc = PgBox::from_pg(pg_sys::CreateTupleDescCopy(relation.rd_att));
            let tupdesc = PgTupleDesc::from_pg(pg_sys::CreateTupleDescCopy(relation.rd_att));
            let rdata = PgBox::from_pg(relation.rd_rel);
            let relname = CStr::from_ptr(rdata.relname.data.as_ptr())
                .to_string_lossy()
                .into_owned();
            (relname, tupdesc)
        };

        let redo_query = match heap_op {
            pg_sys::XLOG_HEAP_INSERT => {
                self.apply_heap_insert(page, blk);
                let newtup = self.get_heap_tuple(page, relid, false).unwrap();
                let heap_tuple = unsafe { PgHeapTuple::from_heap_tuple(tupdesc, newtup.into_pg()) };
                Some(tuple_str::generate_insert_query(&relname, &heap_tuple))
            }
            pg_sys::XLOG_HEAP_UPDATE | pg_sys::XLOG_HEAP_DELETE => todo!("Heap update and delete"),
            _ => return None,
        };

        Some(DecodedResult {
            lsn: self.record.lsn.cast_signed(),
            dboid: blk.rlocator.dbOid,
            relid,
            xid: self.record.header.xl_xid,
            redo_query,
            revert_query: None,
            row_before: None,
            row_after: None,
        })
    }

    fn get_xlrec<T>(&self) -> PgBox<T> {
        unsafe { PgBox::from_pg(self.record.main_data.cast()) }
    }

    fn get_heap_op(&self) -> u32 {
        u32::from(self.record.header.xl_info) & pg_sys::XLOG_HEAP_OPMASK
    }

    /// Apply the current heap insert record
    fn apply_heap_insert(
        &self,
        page: &PgBox<pg_sys::PageHeaderData>,
        blk: &PgBox<DecodedBkpBlock>,
    ) {
        if blk.has_image && blk.apply_image {
            // This was a FPW, nothing to do
            return;
        }

        let xlrec: PgBox<pg_sys::xl_heap_insert> = self.get_xlrec();

        assert!(
            usize::from(xlrec.offnum) <= SIZE_OF_PAGE_HEADER_DATA,
            "invalid max offset number, {0} > {SIZE_OF_PAGE_HEADER_DATA}",
            xlrec.offnum
        );

        let data_len = usize::from(blk.data_len);
        let block_data = get_block_data(blk).unwrap();
        let data = unsafe { std::slice::from_raw_parts(block_data, data_len) };

        // Data block stores only part of the tuple header.
        let xlhdr: PgBox<pg_sys::xl_heap_header> =
            unsafe { PgBox::from_pg(block_data.cast::<pg_sys::xl_heap_header>()) };

        info!("xlhdr: {xlhdr:?}");
        let new_len = data_len - SIZE_OF_HEAP_HEADER;
        assert!(data_len > SIZE_OF_HEAP_HEADER && new_len <= MAX_HEAP_TUPLE_SIZE);

        let mut htup_vec = vec![0i8; MAX_HEAP_TUPLE_SIZE];
        htup_vec[SIZE_OF_HEAP_HEADER..SIZE_OF_HEAP_HEADER + new_len]
            .copy_from_slice(&data[0..SIZE_OF_HEAP_HEADER]);
        let mut htup =
            unsafe { Box::from_raw(htup_vec.as_mut_ptr().cast::<pg_sys::HeapTupleHeaderData>()) };
        htup.t_infomask2 = xlhdr.t_infomask2;
        htup.t_infomask = xlhdr.t_infomask;
        htup.t_hoff = xlhdr.t_hoff;
        htup.t_choice.t_heap.t_xmin = self.record.header.xl_xid;
        htup.t_choice.t_heap.t_field3.t_cid = 0;
        htup.t_infomask &= !u16::try_from(pg_sys::HEAP_COMBOCID).unwrap();

        unsafe {
            if pg_sys::PageAddItemExtended(
                page.as_ptr().cast::<i8>(),
                Box::into_raw(htup).cast(),
                new_len,
                xlrec.offnum,
                (pg_sys::PAI_OVERWRITE | pg_sys::PAI_IS_HEAP)
                    .try_into()
                    .unwrap(),
            ) == pg_sys::InvalidOffsetNumber
            {
                panic!("failed to add tuple");
            }
        }
    }

    fn get_heap_rec_offnum(&self, old: bool) -> Option<pg_sys::OffsetNumber> {
        let heap_op = self.get_heap_op();
        match heap_op {
            pg_sys::XLOG_HEAP_INSERT => {
                if old {
                    return None;
                }
                Some(self.get_xlrec::<pg_sys::xl_heap_insert>().offnum)
            }
            pg_sys::XLOG_HEAP_DELETE => {
                if !old {
                    return None;
                }
                Some(self.get_xlrec::<pg_sys::xl_heap_delete>().offnum)
            }
            pg_sys::XLOG_HEAP_HOT_UPDATE | pg_sys::XLOG_HEAP_UPDATE => {
                let xlrec = self.get_xlrec::<pg_sys::xl_heap_update>();
                if old {
                    Some(xlrec.old_offnum)
                } else {
                    Some(xlrec.new_offnum)
                }
            }
            e => panic!("Unknow heap op {e}"),
        }
    }

    fn get_heap_tuple(
        &self,
        page: &PgBox<pg_sys::PageHeaderData>,
        relid: pg_sys::Oid,
        old: bool,
    ) -> Option<PgBox<pg_sys::HeapTupleData>> {
        let offnum = self.get_heap_rec_offnum(old)?;

        let item_id = page::get_item_id(page, offnum.into());
        let htuple = page::get_item(page, &item_id).cast();
        let htup_len = item_id.lp_len();

        let mut tuple = unsafe {
            PgBox::<pg_sys::HeapTupleData>::from_pg(
                pg_sys::palloc0(HEAP_TUPLE_SIZE + (htup_len as usize)).cast(),
            )
        };
        tuple.t_data = htuple;
        tuple.t_len = htup_len;
        item::pointer_set_invalid(tuple.t_self);
        tuple.t_tableOid = relid;
        Some(tuple)
    }
}
