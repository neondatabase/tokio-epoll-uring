//! Parent module for all the [`Op`]s that this trait supports.

#[doc(inline)]
pub use crate::system::submission::op_fut::Op;

pub mod fsync;
pub mod nop;
pub mod open_at;
pub mod read;
pub mod statx;
pub mod write;
