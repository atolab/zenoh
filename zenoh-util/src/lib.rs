pub mod sync;
pub mod collections;

// This macro performs an async lock on Mutex<T>
// For performance reasons, it first performs a try_lock() and,
// if it failes, it falls back on lock().await
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

// This macro is a shorthand for performing a read access on a RwLock<Option<T>>
// This macro assumes that no write operations ever occurs on the struct
#[macro_export]
macro_rules! zrwopt {
    ($var:expr) => ($var.try_read().unwrap().as_ref().unwrap());
}