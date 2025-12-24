use pgrx::pg_sys::{XLogLongPageHeaderData, XLOGDIR, XLOG_BLCKSZ};
use std::{
    env,
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
};
use thiserror::Error;

const XLOG_FNAME_LEN: usize = 24;
const WAL_SEG_MIN_SIZE: u32 = 1024 * 1024;
const WAL_SEG_MAX_SIZE: u32 = 1024 * 1024 * 1024;

#[derive(Clone, Debug, Hash, Ord, PartialOrd, PartialEq, Eq, Error)]
pub enum InvalidWalFile {
    #[error("Io error: {0}")]
    IoError(String),
    #[error("Invalid WAL file name {0}")]
    InvalidFileName(String),
    #[error("Could not read WAL file {0}: {1}")]
    ReadError(String, String),
    #[error("WAL file {0} doesn't exist")]
    NoFile(String),
    #[error("Invalid WAL segment size {0}. The WAL segment size must be a power of two between 1MB and 1GB.")]
    InvalidWalSegSz(u32),
}

/// Search if directory contains a valid WAL file.
pub fn search_directory(dir: &PathBuf) -> Result<Option<(PathBuf, u32)>, io::Error> {
    let mut entries = fs::read_dir(dir)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    entries.sort();
    for f in entries {
        if let Ok(segsz) = validate_wal_file(&f) {
            return Ok(Some((f, segsz)));
        }
    }
    Ok(None)
}

/// Validate that the provided file is a valid WAL file
pub fn validate_wal_file(wal_path: &PathBuf) -> Result<u32, InvalidWalFile> {
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
    // Validate segsz from the WAL file
    get_wal_segsz(wal_path)
}

/// Returns true if WAL seg size is correct
pub fn is_wal_segsz_valid(wal_seg_size: u32) -> bool {
    wal_seg_size.is_power_of_two() && (WAL_SEG_MIN_SIZE..=WAL_SEG_MAX_SIZE).contains(&wal_seg_size)
}

/// Identify the target directory.
///
/// Try to find the file in several places:
/// if directory != NULL:
///  directory /
///  directory / XLOGDIR /
/// else
///  .
///  XLOGDIR /
///  $PGDATA / XLOGDIR /
pub fn detect_wal_dir(wal_dir: Option<&str>) -> Option<(PathBuf, u32)> {
    let xlog_dir = XLOGDIR.to_string_lossy().to_string();
    let wal_dir_candidates = if let Some(d) = wal_dir {
        let d = Path::new(d).to_path_buf();
        //  directory /
        //  directory / XLOGDIR /
        vec![d.clone(), d.join(xlog_dir)]
    } else {
        let cwd = Path::new(".").to_path_buf();
        //  .
        //  XLOGDIR /
        let mut r = vec![cwd.clone(), cwd.join(&xlog_dir)];
        let pg_data = env::var("PGDATA");
        if let Ok(pg_data) = pg_data {
            //  $PGDATA / XLOGDIR /
            r.push(Path::new(&pg_data).join(&xlog_dir));
        }
        r
    };

    for d in wal_dir_candidates {
        let f = search_directory(&d);
        if let Ok(Some((_, segsz))) = f {
            return Some((d, segsz));
        }
    }
    None
}

/// Extract wal segsz from wal file
pub fn get_wal_segsz(wal_path: &PathBuf) -> Result<u32, InvalidWalFile> {
    let wal_str = wal_path.to_string_lossy().to_string();
    let mut f = match File::open(wal_path) {
        Ok(f) => f,
        Err(e) => return Err(InvalidWalFile::ReadError(wal_str, e.to_string())),
    };

    let mut buffer = [0; XLOG_BLCKSZ as usize];
    match f.read_exact(&mut buffer) {
        Ok(r) => r,
        Err(e) => return Err(InvalidWalFile::ReadError(wal_str, e.to_string())),
    }

    let s = unsafe { std::ptr::read(buffer.as_ptr().cast::<XLogLongPageHeaderData>()) };
    if !is_wal_segsz_valid(s.xlp_seg_size) {
        return Err(InvalidWalFile::InvalidWalSegSz(s.xlp_seg_size));
    }
    Ok(s.xlp_seg_size)
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use std::path::Path;

    use crate::wal::{search_directory, validate_wal_file};

    macro_rules! test_path {
        ($dirname:expr) => {
            Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/resources/test/",
                $dirname
            ))
            .to_path_buf()
        };
    }

    #[test]
    fn test_validate_wal_file() {
        let wal_path = test_path!("18_single_upgrade/000000010000000000000018");
        let seg_size = match validate_wal_file(&wal_path) {
            Ok(s) => s,
            Err(e) => panic!("{}", e),
        };
        assert_eq!(seg_size, 1024 * 1024, "Invalid segment size");
    }

    #[test]
    fn test_search_directory() {
        let wal_dir = test_path!("18_single_upgrade");

        let res = search_directory(&wal_dir);
        assert!(res.is_ok());
        let f = res.unwrap().unwrap();
        let expected_path = test_path!("18_single_upgrade/000000010000000000000018");
        assert_eq!(f, (expected_path, 1024 * 1024));
    }
}
