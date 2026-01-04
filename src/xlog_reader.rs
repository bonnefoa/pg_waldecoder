use pgrx::{
    pg_sys::{self, RelFileLocator, XLogRecGetBlockTag},
    PgBox,
};

/// Get block tag info from latest decoded record
pub fn get_block_tag(xlog_reader: &PgBox<pg_sys::XLogReaderState>) -> (RelFileLocator, i32, u32) {
    let mut rlocator: RelFileLocator = RelFileLocator {
        spcOid: 0.into(),
        dbOid: 0.into(),
        relNumber: 0.into(),
    };
    let mut forknum: i32 = 0;
    let mut blknum: u32 = 0;
    unsafe {
        XLogRecGetBlockTag(
            xlog_reader.as_ptr(),
            0,
            &raw mut rlocator,
            &raw mut forknum,
            &raw mut blknum,
        );
    };
    (rlocator, forknum, blknum)
}

pub fn get_block(
    record: &mut PgBox<pg_sys::DecodedXLogRecord>,
    blkid: u8,
) -> PgBox<pg_sys::DecodedBkpBlock> {
    unsafe { PgBox::from_pg(record.blocks.as_mut_ptr().add(blkid as usize)) }
}
