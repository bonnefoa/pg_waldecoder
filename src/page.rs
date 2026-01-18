use pgrx::{
    PgBox, pg_sys
};

pub fn get_item_id(page: &PgBox<pg_sys::PageHeaderData>, offnum: usize) -> pg_sys::ItemIdData {
    unsafe { page.pd_linp.as_slice(offnum)[offnum - 1] }
}

pub fn get_item(page: &PgBox<pg_sys::PageHeaderData>, item_id: &pg_sys::ItemIdData) -> pg_sys::Item {
    let offset = item_id.lp_len();
    unsafe { page.as_ptr().add(offset as usize).cast() }
}
