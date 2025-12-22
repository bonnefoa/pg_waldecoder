use std::{fs::File, io::Read, path::Path};
use pgrx::pg_sys::{XLOG_BLCKSZ, XLogLongPageHeaderData};
use thiserror::Error;

const XLOG_FNAME_LEN: usize = 24;

#[derive(Clone, Debug, Hash, Ord, PartialOrd, PartialEq, Eq, Error)]
pub enum InvalidWalFile {
    #[error("Could not locate director {0}")]
    NoDir(String),
    #[error("Invalid WAL file name {0}")]
    InvalidFileName(String),
    #[error("Could not read WAL file {0}: {1}")]
    ReadError(String, String),
    #[error("WAL file {0} doesn't exist")]
    NoFile(String),
    #[error("Invalid WAL segment size {0}. The WAL segment size must be a power of two between 1MB and 1GB.")]
    InvalidWalSegSz(u32),
}

// pub fn detect_directory(dir: Option<String>, fname: String) -> Result<String,InvalidWalFile> {
//     match dir {
//         Ok(dir) => {
//         },
//         None => {
//         },
//     };
//     todo!()
// }

pub fn validate_wal_file(wal_path: &Path) -> Result<u32, InvalidWalFile> {
    let wal_str = wal_path.to_string_lossy().to_string();
    if !wal_path.exists() {
        return Err(InvalidWalFile::NoFile(wal_str));
    }

    // Extract file
    let Some(file_name) = wal_path.file_name().and_then(|f| f.to_str()) else {
        return Err(InvalidWalFile::NoFile(wal_str));
    };

    // We should have 24 characters
    if file_name.len() != XLOG_FNAME_LEN {
        return Err(InvalidWalFile::InvalidFileName(file_name.to_string()));
    }

    // With only hexadecimal characters
    for c in file_name.chars() {
        if !c.is_ascii_hexdigit() {
            return Err(InvalidWalFile::InvalidFileName(file_name.to_string()));
        }
    }

    let mut f = match File::open(wal_path) {
        Ok(f) => f,
        Err(e) => return Err(InvalidWalFile::ReadError(wal_str, e.to_string())),
    };

    let mut buffer = [0; XLOG_BLCKSZ as usize];
    match f.read_exact(&mut buffer) {
        Ok(r) => r,
        Err(e) => return Err(InvalidWalFile::ReadError(wal_str, e.to_string())),
    }

    let s = unsafe { std::ptr::read(buffer.as_ptr() as *const XLogLongPageHeaderData) };
    Ok(s.xlp_seg_size)
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use std::path::Path;

    use crate::wal_utils::validate_wal_file;

    macro_rules! test_case {
        ($dirname:expr) => {
            concat!(env!("CARGO_MANIFEST_DIR"), "/resources/test/", $dirname)
        };
    }

    #[test]
    fn test_validate_wal_file() {
        let wal_path = Path::new(test_case!("18_single_upgrade/000000010000000000000018"));
        let seg_size = match validate_wal_file(wal_path) {
            Ok(s) => s,
            Err(e) => panic!("{}", e),
        };

        assert_eq!(seg_size, 1024 * 1024, "Invalid segment size");
    }

    //    #[test]
    //    fn test_lsn_to_startptr() {
    //        let res = crate::lsn_to_rec_ptr("0/01800C50");
    //        assert_eq!(res.unwrap(), 25168976);
    //        let res = crate::lsn_to_rec_ptr("2/01800C50");
    //        assert_eq!(res.unwrap(), 8615103568);
    //    }
}
