use pgrx::callconv::{ArgAbi, BoxRet};
use pgrx::datum::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};
use pgrx::prelude::*;
use pgrx::{rust_regtypein, StringInfo};
use std::ffi::CStr;
use std::fmt::{Display, Formatter};
use std::num::TryFromIntError;
use std::ops::{Add, Sub};
use std::path;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct PgLSN {
    value: u64,
}

#[derive(Clone, Debug, Hash, Ord, PartialOrd, PartialEq, Eq, thiserror::Error)]
pub enum InvalidLSN {
    #[error("Invalid LSN Format '{0}'")]
    Format(String),
    #[error("Invalid filename: '{0}'")]
    FileName(String),
    #[error("Invalid hex value in '{0}': `{1}`")]
    HexValue(String, String),
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

impl TryFrom<&str> for PgLSN {
    type Error = InvalidLSN;

    fn try_from(lsn: &str) -> Result<Self, Self::Error> {
        let mut iter = lsn.split('/');
        let Some(xlogid_str) = iter.next() else {
            return Err(InvalidLSN::Format(lsn.to_string()));
        };
        let xlogid = match u64::from_str_radix(xlogid_str, 16) {
            Ok(xlogid) => xlogid,
            Err(e) => return Err(InvalidLSN::HexValue(lsn.to_string(), e.to_string())),
        };

        let xrecoff_str = iter.next().unwrap();
        let xrecoff = match u64::from_str_radix(xrecoff_str, 16) {
            Ok(xrecoff) => xrecoff,
            Err(e) => return Err(InvalidLSN::HexValue(lsn.to_string(), e.to_string())),
        };
        Ok(PgLSN::from(xlogid << 32 | xrecoff))
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

/// Returns file name for a provided timeline and record pointer
pub fn xlog_file_name(tli: pg_sys::TimeLineID, log_seg_no: pg_sys::XLogSegNo, wal_segsz_bytes: i32) -> String {
    let segments_per_xlog_id = 0x100000000u64 / u64::from(wal_segsz_bytes.cast_unsigned());
    let up = log_seg_no / segments_per_xlog_id;
    let rest = log_seg_no % segments_per_xlog_id;
    format!("{tli:08X}{up:08X}{rest:08X}")
}

/// Convert a filename to a start ptr and timeline
pub fn filename_to_startptr(
    filename: &str,
    wal_segsz_bytes: u64,
) -> Result<(u64, u64), InvalidLSN> {
    let Some(filename) = path::Path::new(filename)
        .file_name()
        .and_then(|s| s.to_str())
    else {
        return Err(InvalidLSN::FileName(filename.to_string()));
    };

    let tli_str = &filename[0..8];
    let tli = match u64::from_str_radix(tli_str, 16) {
        Ok(tli) => tli,
        Err(e) => {
            return Err(InvalidLSN::HexValue(
                filename[0..8].to_string(),
                e.to_string(),
            ))
        }
    };

    let log_str = &filename[8..16];
    let log = match u64::from_str_radix(log_str, 16) {
        Ok(log) => log,
        Err(e) => return Err(InvalidLSN::HexValue(log_str.to_string(), e.to_string())),
    };

    let seg_str = &filename[16..24];
    let seg = match u64::from_str_radix(seg_str, 16) {
        Ok(seg) => seg,
        Err(e) => return Err(InvalidLSN::HexValue(seg_str.to_string(), e.to_string())),
    };
    Ok((tli, log * 0x100000000 * wal_segsz_bytes + seg))
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::pg_lsn::{PgLSN, filename_to_startptr, xlog_file_name};


    #[test]
    fn test_str_to_pglsn() {
        let res = PgLSN::try_from("0/01800C50");
        assert_eq!(res.unwrap(), PgLSN::from(0x1800c50_u64));
        let res = PgLSN::try_from("2/01800C50");
        assert_eq!(res.unwrap(), PgLSN::from(0x201800c50_u64));
    }

    #[test]
    fn test_filename_to_startptr() {
        let res = filename_to_startptr("000000010000000000000018", 1024 * 1024);
        assert_eq!(res.unwrap(), (1, 24));
    }

    #[test]
    fn test_xlog_file_name() {
        let res = xlog_file_name(1, 0x18, 1024 * 1024);
        assert_eq!(res, "000000010000000000000018");
    }
}
