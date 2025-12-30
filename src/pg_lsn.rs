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
use std::num::TryFromIntError;
use std::ops::{Add, Sub};

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct PgLSN {
    value: u64,
}

impl Display for PgLSN {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // format ourselves as a `ffffffff/ffffffff` string
        write!(
            f,
            "{0:X}/{1:08X}",
            self.value >> 32,
            (self.value & 0xffffffff) as u32
        )
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
            Some(PgLSN {
                value: datum.value() as _,
            })
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

impl From<i32> for PgLSN {
    fn from(value: i32) -> Self {
        PgLSN { value: value.cast_unsigned().into() }
    }
}

impl From<u32> for PgLSN {
    fn from(value: u32) -> Self {
        PgLSN { value: value.into() }
    }
}

impl From<u64> for PgLSN {
    fn from(value: u64) -> Self {
        PgLSN { value }
    }
}

impl From<PgLSN> for u64 {
    fn from(value: PgLSN) -> Self {
        value.value
    }
}

impl TryFrom<PgLSN> for u32 {
    type Error = TryFromIntError;

    fn try_from(value: PgLSN) -> Result<Self, Self::Error> {
        u32::try_from(value.value)
    }
}

impl Add<u32> for PgLSN {
    type Output = Self;
    fn add(self, rhs: u32) -> Self::Output {
        PgLSN {
            value: self.value + u64::from(rhs),
        }
    }
}

impl Add<i32> for PgLSN {
    type Output = Self;
    fn add(self, rhs: i32) -> Self::Output {
        PgLSN {
            value: self.value + u64::from(rhs.cast_unsigned()),
        }
    }
}

impl Add<u64> for PgLSN {
    type Output = Self;
    fn add(self, rhs: u64) -> Self::Output {
        PgLSN {
            value: self.value + rhs,
        }
    }
}

impl Sub<u64> for PgLSN {
    type Output = Self;
    fn sub(self, rhs: u64) -> Self::Output {
        PgLSN {
            value: self.value - rhs,
        }
    }
}

impl Sub<PgLSN> for PgLSN {
    type Output = Self;

    fn sub(self, rhs: PgLSN) -> Self::Output {
        PgLSN {
            value: self.value - rhs.value,
        }
    }
}

