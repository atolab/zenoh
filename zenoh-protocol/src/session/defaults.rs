use crate::core::ZInt;

// Default session lease in milliseconds
pub(crate) const SESSION_LEASE: u64 = 30_000;

// The default sequence number resolution takes 4 bytes on the wire: 28 useful bits
// 2^28 = 268_435_456 => Max Seq Num = 268_435_456
pub(crate) const SESSION_SEQ_NUM_RESOLUTION: ZInt = 268_435_456;

// The default batch size in bytes for the transport
pub(crate) const SESSION_BATCH_SIZE: usize = 8_192;

// Default timeout when opening a session in milliseconds
pub(crate) const SESSION_OPEN_TIMEOUT: u64 = 10_000;

// Parameters of the conduit transmission queue
pub(crate) const QUEUE_PRIO_CTRL: usize = 0;
pub(crate) const QUEUE_SIZE_CTRL: usize = 16;
pub(crate) const QUEUE_CRED_CTRL: isize = 1;

pub(crate) const _QUEUE_PRIO_RETX: usize = 1;
pub(crate) const QUEUE_SIZE_RETX: usize = 64;
pub(crate) const QUEUE_CRED_RETX: isize = 1;

pub(crate) const QUEUE_PRIO_DATA: usize = 2;
pub(crate) const QUEUE_SIZE_DATA: usize = 256;
pub(crate) const QUEUE_CRED_DATA: isize = 100;

pub(crate) const QUEUE_SIZE_TOT: usize = QUEUE_SIZE_CTRL + QUEUE_SIZE_RETX + QUEUE_SIZE_DATA;
pub(crate) const QUEUE_CONCURRENCY: usize = 16;

pub(crate) const WRITE_MSG_SLICE_SIZE: usize = 128;