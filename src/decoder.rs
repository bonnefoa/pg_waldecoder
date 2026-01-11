use std::collections::HashMap;
use std::ffi::CStr;
use std::fmt::{Display, Formatter};

use pgrx::pg_sys::{CurrentMemoryContext, DecodedBkpBlock, InvalidXLogRecPtr, Oid, PGAlignedBlock};
use pgrx::{
    error,
    ffi::c_char,
    pg_sys::{self, RmgrIds::RM_HEAP_ID},
    PgBox,
};
use pgrx::{info, warning, AllocatedByRust, PgMemoryContexts};

use crate::pg_lsn::PgLSN;
use crate::relation::get_relid_from_rlocator;
use crate::xlog_reader;

pub struct DecodedResult {
    pub lsn: i64,
    pub dboid: pg_sys::Oid,
    pub relid: pg_sys::Oid,
    pub xid: pg_sys::TransactionId,
    pub redo_query: Option<&'static str>,
    pub revert_query: Option<&'static str>,
    pub row_before: Option<&'static str>,
    pub row_after: Option<&'static str>,
}

impl From<DecodedResult>
    for (
        i64,
        pg_sys::Oid,
        pg_sys::Oid,
        pg_sys::TransactionId,
        Option<&'static str>,
        Option<&'static str>,
        Option<&'static str>,
        Option<&'static str>,
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
    page_hash: HashMap<PageId, PgBox<pg_sys::PageHeaderData>>,
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
        // let function_ctx = unsafe { per_record_ctx.parent().unwrap() };
        let function_ctx = PgMemoryContexts::CurrentMemoryContext;

        // Check we have can find valid wal files
        let first_record =
            unsafe { pg_sys::XLogFindNextRecord(xlog_reader.as_ptr(), startptr.into()) };
        if first_record == u64::from(InvalidXLogRecPtr) {
            error!("could not find a valid record after {}", startptr);
        }

        let page_hash = HashMap::new();
        let record = unsafe { PgBox::from_pg(xlog_reader.record) };
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

    fn restore_fpw(
        &self,
        blk_id: u8,
        blk: &PgBox<DecodedBkpBlock>,
    ) -> Option<PgBox<pg_sys::PageHeaderData>> {
        if !blk.has_image || !blk.apply_image {
            // No FPW to restore
            return None;
        }
        // Yes, create the page and insert it
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
        Some(unsafe { PgBox::from_pg(page.as_ptr().cast::<pg_sys::PageHeaderData>()) })
    }

    /// Get block at index `blk_id` for the current record
    fn get_block(&mut self, blk_id: u8) -> PgBox<pg_sys::DecodedBkpBlock> {
        unsafe { PgBox::from_pg(self.record.blocks.as_mut_ptr().add(blk_id as usize)) }
    }

    fn process_current_record(&mut self) -> Option<DecodedResult> {
        let rmid = u32::from(self.record.header.xl_rmid);
        if rmid != RM_HEAP_ID {
            // Move to the next record
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
        let blk = self.get_block(blk_id);

        // Do we have a FPW to apply?
        let page_id = PageId::new(&blk);
        if let Some(page) = self.restore_fpw(blk_id, &blk) {
            // Insert it
            info!("Found a FPW for page_id {page_id}");
            self.page_hash.insert(page_id.clone(), page);
        }

        let Some(page) = self.page_hash.get(&page_id) else {
            warning!("No page found for {page_id}, skipping record");
            return None;
        };

        let Some(relid) = get_relid_from_rlocator(&blk.rlocator) else {
            pg_sys::warning!("Couldn't find oid for rlocator {:?}", blk.rlocator);
            return None;
        };

        match rmid {
            RM_HEAP_ID => self.decode_heap_record(page, &blk, relid),
            _ => panic!("Unsupported record type"),
        }
    }

    pub fn decode_heap_record(
        &self,
        page: &PgBox<pg_sys::PageHeaderData>,
        blk: &PgBox<DecodedBkpBlock>,
        relid: Oid,
    ) -> Option<DecodedResult> {
        let heap_op = u32::from(self.record.header.xl_info) & pg_sys::XLOG_HEAP_OPMASK;
        let op_name = unsafe { pg_sys::heap_identify(heap_op.try_into().unwrap()) };
        let op_name_str = unsafe { CStr::from_ptr(op_name).to_str().unwrap() };
        pg_sys::info!(
            "Processing HEAP record {} at LSN {}",
            op_name_str,
            self.xlog_reader.ReadRecPtr
        );

        match heap_op {
            pg_sys::XLOG_HEAP_INSERT => self.apply_heap_insert(page, blk),
            pg_sys::XLOG_HEAP_UPDATE | pg_sys::XLOG_HEAP_DELETE => todo!("Heap update and delete"),
            _ => return None,
        }

        Some(DecodedResult {
            lsn: self.record.lsn.cast_signed(),
            dboid: blk.rlocator.dbOid,
            relid,
            xid: self.record.header.xl_xid,
            redo_query: None,
            revert_query: None,
            row_before: None,
            row_after: None,
        })
    }

    fn apply_heap_insert(
        &self,
        page: &PgBox<pg_sys::PageHeaderData>,
        blk: &PgBox<DecodedBkpBlock>,
    ) {
        if blk.has_image && blk.apply_image {
            // This was a FPW, nothing to do
            return;
        }
    }
}
