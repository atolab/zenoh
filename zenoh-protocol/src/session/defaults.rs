use crate::core::ZInt;

configurable! {
    // Default session lease in seconds
    pub(crate) static ref SESSION_LEASE: ZInt = 30;

    // The default sequence number resolution takes 4 bytes on the wire: 28 useful bits
    // 2^28 = 268_435_456 => Max Seq Num = 268_435_455
    pub(crate) static ref SESSION_SEQ_NUM_RESOLUTION: ZInt = 268_435_456;

    // The default batch size in bytes for the transport
    pub(crate) static ref SESSION_BATCH_SIZE: usize = 8_192;

    // Default timeout when opening a session in milliseconds
    pub(crate) static ref SESSION_OPEN_TIMEOUT: u64 = 10_000;

    // Default retries when opening a session
    pub(crate) static ref SESSION_OPEN_RETRIES: usize = 3;

    // Parameters of the conduit transmission queue
    pub(crate) static ref QUEUE_PRIO_CTRL: usize = 0;
    pub(crate) static ref QUEUE_SIZE_CTRL: usize = 16;
    pub(crate) static ref QUEUE_CRED_CTRL: isize = 1;

    pub(crate) static ref QUEUE_PRIO_RETX: usize = 1;
    pub(crate) static ref QUEUE_SIZE_RETX: usize = 64;
    pub(crate) static ref QUEUE_CRED_RETX: isize = 1;

    pub(crate) static ref QUEUE_PRIO_DATA: usize = 2;
    pub(crate) static ref QUEUE_SIZE_DATA: usize = 1_024;
    pub(crate) static ref QUEUE_CRED_DATA: isize = 100;

    pub(crate) static ref QUEUE_SIZE_TOT: usize = *QUEUE_SIZE_CTRL + *QUEUE_SIZE_RETX + *QUEUE_SIZE_DATA;
    pub(crate) static ref QUEUE_CONCURRENCY: usize = 16;

    pub(crate) static ref WRITE_MSG_SLICE_SIZE: usize = 128;
}
