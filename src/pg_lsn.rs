use pgrx::callconv::{ArgAbi, BoxRet};
use pgrx::datum::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};
use pgrx::prelude::*;
use pgrx::{rust_regtypein, StringInfo};
use std::error::Error;
use std::ffi::CStr;
use std::fmt::{Display, Formatter};

#[repr(transparent)]
#[derive(
    Copy,
    Clone,
    Debug,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash,
)]
pub struct PgLSN {
    value: u64,
}

impl Display for PgLSN {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // format ourselves as a `ffffffff/ffffffff` string
        write!(f, "{0:X}/{1:08X}", self.value >> 32, (self.value & 0xffffffff) as u32)
    }
}

unsafe impl SqlTranslatable for PgLSN {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        // this is what the SQL type is called when used in a function argument position
        Ok(SqlMapping::As("pg_lsn".into()))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        // this is what the SQL type is called when used in a function return type position
        Ok(Returns::One(SqlMapping::As("pg_lsn".into())))
    }
}

impl FromDatum for PgLSN {
    unsafe fn from_polymorphic_datum(datum: pg_sys::Datum, is_null: bool, _: Oid) -> Option<Self>
    where
        Self: Sized,
    {
        if is_null {
            None
        } else {
            Some(PgLSN { value: datum.value() as _ })
        }
    }
}

impl IntoDatum for PgLSN {
    fn into_datum(self) -> Option<pg_sys::Datum> {
        Some(pg_sys::Datum::from(self.value))
    }

    fn type_oid() -> Oid {
        pg_sys::PG_LSNOID
    }
}

unsafe impl<'fcx> ArgAbi<'fcx> for PgLSN
where
    Self: 'fcx,
{
    unsafe fn unbox_arg_unchecked(arg: ::pgrx::callconv::Arg<'_, 'fcx>) -> Self {
        unsafe { arg.unbox_arg_using_from_datum().unwrap() }
    }
}

unsafe impl BoxRet for PgLSN {
    unsafe fn box_into<'fcx>(self, fcinfo: &mut pgrx::callconv::FcInfo<'fcx>) -> Datum<'fcx> {
        unsafe { fcinfo.return_raw_datum(pg_sys::Datum::from(self.value)) }
    }
}
