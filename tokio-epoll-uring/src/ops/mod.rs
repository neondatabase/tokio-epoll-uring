//! Parent module for all the [`Op`]s that this trait supports.

#[doc(inline)]
pub use crate::system::submission::op_fut::Op;

pub mod nop;
pub mod read;
