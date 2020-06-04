#![feature(const_if_match)]

pub mod sync;
pub mod collections;
pub mod plugins;

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

// This macro performs an async send on Channel<T>
// For performance reasons, it first performs a try_send() and,
// if it fails, it falls back on send().await
#[macro_export]
macro_rules! zasyncsend {
    ($ch:expr, $var:expr) => (
        if $ch.try_send($var).is_err() { 
            $ch.send($var).await;
        }
    );
}

// This macro performs an async recv on Channel<T>
// For performance reasons, it first performs a try_recv() and,
// if it fails, it falls back on recv().await
#[macro_export]
macro_rules! zasyncrecv {
    ($ch:expr) => (
        if let Ok(v) = $ch.try_recv() { 
            Ok(v)
        } else {
            $ch.recv().await
        }
    );
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