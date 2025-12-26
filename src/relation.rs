use pgrx::{
    pg_sys::{self, InvalidOid, Oid},
    prelude::*,
    Spi,
};

/// Find the matching relid for the provided `RelFileLocator`
pub fn get_relid_from_rlocator(rlocator: &pg_sys::RelFileLocator) -> Option<Oid> {
    let tablespace = if rlocator.spcOid == pg_sys::DEFAULTTABLESPACE_OID {
        InvalidOid
    } else {
        rlocator.spcOid
    };
    // pgrx would convert invalid oid to null, thus we need to manually build the datum
    let tbl_arg = pg_sys::Datum::from(tablespace);
    match Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT oid FROM pg_class where relfilenode=$1 AND reltablespace=$2",
        &[rlocator.relNumber.into(), tbl_arg.into()],
    ) {
        Ok(oid) => oid,
        Err(e) => error!(
            "Couldn't get oid for relation relfilnode {}, tablespace {}: {}",
            rlocator.relNumber, rlocator.spcOid, e
        ),
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
