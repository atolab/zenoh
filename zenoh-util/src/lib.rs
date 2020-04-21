#![feature(const_if_match)]

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

// This macro is a shorthand for performing a read access on a RwLock<Option<T>>
// This macro assumes that no write operations ever occurs on the struct
#[macro_export]
macro_rules! zrwopt {
    ($var:expr) => ($var.try_read().unwrap().as_ref().unwrap());
}

// This macro allows to define some compile time configurable static constants
#[macro_export]
macro_rules! configurable {
    ($(#[$attr:meta])* static ref $N:ident : $T:ty = $e:expr; $($t:tt)*) => {
        lazy_static!($(#[$attr])* static ref $N : $T = match option_env!(stringify!($N)) {
            Some(value) => {value.parse().unwrap()}
            None => {$e} 
        };) ; 
        configurable!($($t)*);
    };
    ($(#[$attr:meta])* pub static ref $N:ident : $T:ty = $e:expr; $($t:tt)*) => {
        lazy_static!($(#[$attr])* pub static ref $N : $T = match option_env!(stringify!($N)) {
            Some(value) => {value.parse().unwrap()}
            None => {$e} 
        };) ; 
        configurable!($($t)*);
    };
    ($(#[$attr:meta])* pub ($($vis:tt)+) static ref $N:ident : $T:ty = $e:expr; $($t:tt)*) => {
        lazy_static!($(#[$attr])* pub ($($vis)+) static ref $N : $T = match option_env!(stringify!($N)) {
            Some(value) => {value.parse().unwrap()}
            None => {$e} 
        };) ; 
        configurable!($($t)*);
    };
    () => ()
}