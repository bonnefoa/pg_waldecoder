use pgrx::{
    PgBox, pg_sys
};

pub fn pointer_set_invalid(mut pointer: pg_sys::ItemPointerData)
{
    pointer.ip_blkid.bi_hi = 0xFFFF;
    pointer.ip_blkid.bi_lo = 0xFFFF;
    pointer.ip_posid = pg_sys::InvalidOffsetNumber;
}
