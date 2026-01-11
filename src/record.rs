use pgrx::{PgBox, pg_sys};

pub fn get_block(
    record: &mut PgBox<pg_sys::DecodedXLogRecord>,
    blk_id: u8,
) -> PgBox<pg_sys::DecodedBkpBlock> {
    unsafe { PgBox::from_pg(record.blocks.as_mut_ptr().add(blk_id as usize)) }
}

