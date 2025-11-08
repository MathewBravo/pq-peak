use core::fmt;

pub enum PeakError {
    UnsupportedFileType,
}

impl fmt::Display for PeakError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PeakError::UnsupportedFileType => {
                write!(f, "UNSUPPORTED_FILE_TYPE (.parquet or .pqt only)")
            }
        }
    }
}
