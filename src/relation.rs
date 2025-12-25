use std::{ffi::CStr, mem::MaybeUninit};

use pgrx::{
    info,
    pg_sys::{self, Oid},
    PgBox,
};

/// For a given tablespace and relfilenode, find the matching relid
pub fn get_spc_relnumber_relid(tablespace: Oid, relfilenode: Oid) -> Oid {
    let mut key_1 = MaybeUninit::<pg_sys::ScanKeyData>::uninit();
    let mut key_2 = MaybeUninit::<pg_sys::ScanKeyData>::uninit();
    let lockmode = pg_sys::AccessShareLock.cast_signed();
    let strategy = u16::try_from(pg_sys::BTEqualStrategyNumber).unwrap();
    let procedure: Oid = pg_sys::F_OIDEQ.into();
    unsafe {
        pg_sys::ScanKeyInit(
            key_1.as_mut_ptr(),
            pg_sys::Anum_pg_class_reltablespace.try_into().unwrap(),
            strategy,
            procedure,
            pg_sys::ObjectIdGetDatum(tablespace),
        );
        let key_1 = key_1.assume_init();
        pg_sys::ScanKeyInit(
            key_2.as_mut_ptr(),
            pg_sys::Anum_pg_class_relfilenode.try_into().unwrap(),
            strategy,
            procedure,
            pg_sys::ObjectIdGetDatum(relfilenode),
        );
        let key_2 = key_2.assume_init();
        let mut keys = vec![key_1, key_2];

        let pg_class = pg_sys::table_open(pg_sys::RelationRelationId, lockmode);

        let scan = pg_sys::systable_beginscan(
            pg_class,
            pg_sys::ClassTblspcRelfilenodeIndexId.into(),
            true,
            std::ptr::null_mut(),
            2,
            keys.as_mut_ptr(),
        );

        let tuple = pg_sys::systable_getnext(scan);
        let relid = if tuple.is_null() {
            pg_sys::InvalidOid
        } else {
            let tuple = PgBox::from_pg(tuple);
            let t_data = PgBox::from_pg(tuple.t_data);
            let ptr = tuple
                .t_data
                .add(t_data.t_hoff.into())
                .cast::<pg_sys::FormData_pg_class>();
            let form_pg_class = PgBox::from_pg(ptr);
            form_pg_class.oid
        };
        pg_sys::systable_endscan(scan);

        pg_sys::table_close(pg_class, lockmode);
        relid
    }
}
