// Todo: add this again once we have an alternative to thiserror_lite
// #![no_std] 

pub mod util;

use macro_rules_attribute::apply;
use thiserror_lite::err_enum;

/// The protocol version this client implements:
///
/// `3.0`
const PROTOCOL_VERSION: i32 = 0x00030000;

/// Default postgres server port
const POSTGRES_DEFAULT_PORT: u16 = 5432;

#[apply(err_enum)]
#[derive(Debug)]
pub enum Error {
    #[error("error encoding or decoding a message")]
    CodecError(),
    #[error("error connecting to the server")]
    IoError(std::io::Error)
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}
