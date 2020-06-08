#![feature(const_if_match)]

#[macro_use]
extern crate lazy_static;

pub mod collections;
pub mod core;
pub mod plugins;
pub mod sync;

pub use crate::core::macros::*;

