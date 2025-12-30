use pgrx::pg_sys::{TimeLineID, XLogRecPtr, XLogSegNo};
use std::path;
use thiserror::Error;

use crate::pg_lsn::PgLSN;

#[derive(Clone, Debug, Hash, Ord, PartialOrd, PartialEq, Eq, Error)]
pub enum InvalidLSN {
    #[error("Invalid LSN Format '{0}'")]
    Format(String),
    #[error("Invalid filename: '{0}'")]
    FileName(String),
    #[error("Invalid hex value in '{0}': `{1}`")]
    HexValue(String, String),
}

/// Convert a lsn string to a start ptr
pub fn lsn_to_rec_ptr(lsn: &str) -> Result<PgLSN, InvalidLSN> {
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

/// Returns file name for a provided timeline and record pointer
pub fn format_lsn(rec_ptr: XLogRecPtr) -> String {
    format!(
        "{0:X}/{1:08X}",
        rec_ptr >> 32,
        (rec_ptr & 0xffffffff) as u32
    )
}

/// Returns file name for a provided timeline and record pointer
pub fn xlog_file_name(tli: TimeLineID, log_seg_no: XLogSegNo, wal_segsz_bytes: i32) -> String {
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
    use crate::{lsn::{filename_to_startptr, xlog_file_name}, pg_lsn::PgLSN};

    #[test]
    fn test_lsn_to_startptr() {
        let res = crate::lsn_to_rec_ptr("0/01800C50");
        assert_eq!(res.unwrap(), PgLSN::from(0x1800c50_u64));
        let res = crate::lsn_to_rec_ptr("2/01800C50");
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
