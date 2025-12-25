use pgrx::pg_sys::{self, RelFileLocator, XLogRecGetBlockTag};

/// Get block tag info from latest decoded record
pub fn get_block_tag(xlog_reader: *mut pg_sys::XLogReaderState) -> (RelFileLocator, i32, u32) {
    let mut rlocator: RelFileLocator = RelFileLocator {
        spcOid: 0.into(),
        dbOid: 0.into(),
        relNumber: 0.into(),
    };
    let mut forknum: i32 = 0;
    let mut blknum: u32 = 0;
    unsafe {
        XLogRecGetBlockTag(
            xlog_reader,
            0,
            &raw mut rlocator,
            &raw mut forknum,
            &raw mut blknum,
        );
    };
    (rlocator, forknum, blknum)
}
