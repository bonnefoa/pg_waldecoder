use pgrx::{
    pg_sys::{self, InvalidOid, Oid},
    prelude::*,
    Spi,
};

/// Find the matching relid for the provided `RelFileLocator`
pub fn get_relid_from_rlocator(rlocator: &pg_sys::RelFileLocator) -> Option<Oid> {
    unsafe {
        match pg_sys::RelidByRelfilenumber(rlocator.spcOid, rlocator.relNumber) {
            pg_sys::InvalidOid => None,
            r => Some(r),
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::relation::get_relid_from_rlocator;
    use pgrx::prelude::*;

    #[pg_test]
    fn test_get_relid_from_rlocator() {
        let Ok((Some(expected_oid), Some(relfilenode), Some(tablespace))) =
            Spi::get_three::<pg_sys::Oid, pg_sys::Oid, pg_sys::Oid>(
                "SELECT oid, relfilenode, reltablespace FROM pg_class where relname='pg_statistic'",
            )
        else {
            panic!("Couldn't get relfilenode and tablespace")
        };

        let rlocator = pg_sys::RelFileLocator {
            spcOid: tablespace,
            dbOid: 0.into(),
            relNumber: relfilenode,
        };
        let relid = get_relid_from_rlocator(&rlocator).unwrap();
        assert_eq!(relid, expected_oid);
    }
}
