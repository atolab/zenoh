use std::fmt;

#[derive(Debug, PartialEq)]
pub enum ZErrorKind {
    BufferOverflow { missing: usize },
    BufferUnderflow { missing: usize },
    InvalidMessage { descr: String },
    InvalidLocator { descr: String },
    IOError { descr: String },
    Other { descr: String }
}

impl fmt::Display for ZErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZErrorKind::BufferOverflow { missing } =>
                write!(f, "Failed to write in full buffer ({} bytes missing)", missing),
            ZErrorKind::BufferUnderflow { missing } =>
                write!(f, "Failed to read from empty buffer ({} bytes missing)", 
                  (if *missing == 0 {"some".to_string()} else { missing.to_string() })),
            ZErrorKind::InvalidMessage { descr } =>
                write!(f, "Invalid message ({})", descr),
            ZErrorKind::InvalidLocator { descr } =>
                write!(f, "Invalid locator ({})", descr),
            ZErrorKind::IOError { descr } =>
                write!(f, "IO error ({})", descr),
            ZErrorKind::Other { descr } =>
                write!(f, "zenoh error: \"{}\"", descr),
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
    pub fn new<'a>(kind: ZErrorKind, file:&'static str, line: u32, source: Option<Box<dyn std::error::Error>>) -> ZError {
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
