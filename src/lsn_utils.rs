use std::path;
use thiserror::Error;

#[derive(Clone, Debug, Hash, Ord, PartialOrd, PartialEq, Eq, Error)]
pub enum InvalidLSN {
    #[error("No LSN provided")]
    NoLSN,
    #[error("Invalid filename")]
    InvalidFileName,
    #[error("Invalid hex value in '{0}': `{1}`")]
    InvalidHexValue(String, String),
}

pub fn lsn_to_rec_ptr(lsn: &str) -> Result<u64, InvalidLSN> {
    let mut iter = lsn.split('/');
    let xlogid_str = iter.next().unwrap();
    let xlogid = match u64::from_str_radix(xlogid_str, 16) {
        Ok(xlogid) => xlogid,
        Err(e) => return Err(InvalidLSN::InvalidHexValue(lsn.to_string(), e.to_string())),
    };

    let xrecoff_str = iter.next().unwrap();
    let xrecoff = match u64::from_str_radix(xrecoff_str, 16) {
        Ok(xrecoff) => xrecoff,
        Err(e) => return Err(InvalidLSN::InvalidHexValue(lsn.to_string(), e.to_string())),
    };
    Ok(xlogid << 32 | xrecoff)
}

pub fn filename_to_startptr(
    filename: Option<&str>,
    wal_segsz_bytes: u64,
) -> Result<u64, InvalidLSN> {
    let Some(filename) = filename else {
        return Err(InvalidLSN::NoLSN);
    };

    let Some(filename) = path::Path::new(filename)
        .file_name()
        .and_then(|s| s.to_str())
    else {
        return Err(InvalidLSN::InvalidFileName);
    };

    // let _tli = match u64::from_str_radix(&filename[0..8], 16) {
    //     Ok(tli) => tli,
    //     Err(e) => {
    //         return Err(InvalidLSN::InvalidHexValue(
    //             filename[0..8].to_string(),
    //             e.to_string(),
    //         ))
    //     }
    // };

    let log_str = &filename[8..16];
    let log = match u64::from_str_radix(log_str, 16) {
        Ok(log) => log,
        Err(e) => {
            return Err(InvalidLSN::InvalidHexValue(
                log_str.to_string(),
                e.to_string(),
            ))
        }
    };

    let seg_str = &filename[16..24];
    let seg = match u64::from_str_radix(seg_str, 16) {
        Ok(seg) => seg,
        Err(e) => {
            return Err(InvalidLSN::InvalidHexValue(
                seg_str.to_string(),
                e.to_string(),
            ))
        }
    };
    Ok(log * 0x100000000 * wal_segsz_bytes + seg)
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::lsn_utils::filename_to_startptr;

    #[test]
    fn test_lsn_to_startptr() {
        let res = crate::lsn_to_rec_ptr("0/01800C50");
        assert_eq!(res.unwrap(), 25168976);
        let res = crate::lsn_to_rec_ptr("2/01800C50");
        assert_eq!(res.unwrap(), 8615103568);
    }

    #[test]
    fn test_filename_to_startptr() {
        let res = filename_to_startptr(Some("000000010000000000000018"), 1048576);
        assert_eq!(res.unwrap(), 24);
    }
}
