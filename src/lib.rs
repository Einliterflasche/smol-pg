//! A runtime-agnostic, small footprint client for PostgreSQL.

// Todo: add this again once we have an alternative to thiserror_lite
// #![no_std]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod connection;
pub mod protocol;
pub mod util;

use macro_rules_attribute::apply;
use thiserror_lite::err_enum;

/// The protocol version this client implements:
///
/// - `3.0`
///
/// The first 16 bits are the major version, the next 16 are the minor version.
const PROTOCOL_VERSION: i32 = 0x00030000;

/// Default postgres server port
const POSTGRES_DEFAULT_PORT: u16 = 5432;

/// Errors that can occur when using this library.
#[allow(missing_docs)]
#[apply(err_enum)]
#[derive(Debug)]
pub enum Error {
    #[error("error encoding or decoding a message")]
    CodecError(util::CodecError),
    #[error("error communicating via network")]
    NetworkError(std::io::Error),
    #[error("unexpected message flow")]
    ProtocolError(connection::ProtocolError),
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::NetworkError(value)
    }
}

impl From<util::CodecError> for Error {
    fn from(value: util::CodecError) -> Self {
        Error::CodecError(value)
    }
}

impl From<connection::ProtocolError> for Error {
    fn from(value: connection::ProtocolError) -> Self {
        Error::ProtocolError(value)
    }
}
