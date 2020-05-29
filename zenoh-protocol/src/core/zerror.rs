use std::fmt;
use super::ResourceId;

#[derive(Debug, PartialEq)]
pub enum ZErrorKind {
    BufferOverflow { missing: usize },
    BufferUnderflow { missing: usize },
    InvalidLink { descr: String },
    InvalidLocator { descr: String },
    InvalidMessage { descr: String },
    InvalidResolution { descr: String},
    InvalidSession { descr: String },
    IOError { descr: String },
    Other { descr: String },
    UnkownResourceId { rid: ResourceId }
}

impl fmt::Display for ZErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZErrorKind::BufferOverflow { missing } =>
                write!(f, "Failed to write in full buffer ({} bytes missing)", missing),
            ZErrorKind::BufferUnderflow { missing } =>
                write!(f, "Failed to read from empty buffer ({} bytes missing)", 
                  (if *missing == 0 {"some".to_string()} else { missing.to_string() })),
            ZErrorKind::InvalidLocator { descr } =>
                write!(f, "Invalid locator ({})", descr),
            ZErrorKind::InvalidLink { descr } =>
                write!(f, "Invalid link ({})", descr),
            ZErrorKind::InvalidMessage { descr } =>
                write!(f, "Invalid message ({})", descr),
            ZErrorKind::InvalidResolution { descr} =>
                write!(f, "Invalid Resolution ({})", descr),
            ZErrorKind::InvalidSession { descr } =>
                write!(f, "Invalid Session ({})", descr),
            ZErrorKind::IOError { descr } =>
                write!(f, "IO error ({})", descr),
            ZErrorKind::Other { descr } =>
                write!(f, "zenoh error: \"{}\"", descr),
            ZErrorKind::UnkownResourceId { rid } =>
                write!(f, "Unkown ResourceId ({})", rid),
        }
    }
}

#[derive(Debug)]
pub struct ZErrorNoLife {
    file: &'static str,
}

#[derive(Debug)]
pub struct ZError {
    kind: ZErrorKind,
    file: &'static str,
    line: u32,
    source: Option<Box<dyn std::error::Error>>
}

impl ZError {
    pub fn new(kind: ZErrorKind, file:&'static str, line: u32, source: Option<Box<dyn std::error::Error>>) -> ZError {
        ZError { kind, file, line, source }
    }

    pub fn kind(&self) -> &ZErrorKind {
        &self.kind
    }
}

unsafe impl Send for ZError {}

impl std::error::Error for ZError {
    fn source(&self) -> Option<&'_(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|bx| bx.as_ref())
    }
}

impl fmt::Display for ZError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}:{}.", self.kind.to_string(), self.file, self.line)?;
        if let Some(s) = &self.source {
            write!(f, " - Caused by {}", *s)?;
        }
        Ok(())
    }
}


#[macro_export]
macro_rules! zerror {
    ($kind:expr) => (ZError::new($kind, file!(), line!(), None));
    ($kind:expr, $source:expr) => (ZError::new($kind, file!(), line!(), Some(Box::new($source))));
    ($kind:ident, $descr:expr, $source:expr) => (
        ZError::new(ZErrorKind::$kind{descr:$descr}, file!(), line!(), Some(Box::new($source)));
    )
}

#[macro_export]
macro_rules! to_zerror {
    ($kind:ident, $descr:expr) => (
        |e| { zerror!($kind, $descr, e) }
    )
}
