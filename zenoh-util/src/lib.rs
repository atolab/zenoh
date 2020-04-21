pub mod sync;
pub mod collections;

// This macro performs an async lock on Mutex<T>
// For performance reasons, it first performs a try_lock() and,
// if it fails, it falls back on lock().await
#[macro_export]
macro_rules! zasynclock {
    ($var:expr) => (
        if let Some(g) = $var.try_lock() { 
            g 
        } else { 
            $var.lock().await 
        }
    );
}

// This macro performs an async read on RwLock<T>
// For performance reasons, it first performs a try_read() and,
// if it fails, it falls back on read().await
#[macro_export]
macro_rules! zasyncread {
    ($var:expr) => (
        if let Some(g) = $var.try_read() { 
            g 
        } else { 
            $var.read().await 
        }
    );
}

// This macro performs an async write on RwLock<T>
// For performance reasons, it first performs a try_write() and,
// if it fails, it falls back on write().await
#[macro_export]
macro_rules! zasyncwrite {
    ($var:expr) => (
        if let Some(g) = $var.try_write() { 
            g 
        } else { 
            $var.write().await 
        }
    );
}