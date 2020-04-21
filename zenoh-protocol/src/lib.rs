#![feature(io_slice_advance)]
#[macro_use]
extern crate zenoh_util;
#[macro_use]
extern crate lazy_static;

extern crate async_std;
extern crate byteorder;
extern crate crossbeam;
extern crate uuid;

pub mod core;
pub mod io;
pub mod link;
pub mod proto;
pub mod session;