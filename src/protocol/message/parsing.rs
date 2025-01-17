//! This module contains functions for parsing values from the PostgreSQL protocol.

use crate::util::BoxError;

/// A trait for parsing a value from a query result.
pub trait FromSql<'a>: Sized {
    /// Parse a value from the text representation.
    fn from_text(text: &'a [u8]) -> Result<Self, BoxError>;

    /// Parse a value from the binary representation.
    fn from_binary(binary: &'a [u8]) -> Result<Self, BoxError>;
}

impl<'a> FromSql<'a> for &'a str {
    fn from_text(text: &'a [u8]) -> Result<Self, BoxError> {
        std::str::from_utf8(text).map_err(|e| Box::new(e) as BoxError)
    }

    fn from_binary(binary: &'a [u8]) -> Result<Self, BoxError> {
        std::str::from_utf8(binary).map_err(|e| Box::new(e) as BoxError)
    }
}

impl<'a> FromSql<'a> for String {
    fn from_text(text: &'a [u8]) -> Result<Self, BoxError> {
        Ok(std::str::from_utf8(text)?.to_string())
    }

    fn from_binary(binary: &'a [u8]) -> Result<Self, BoxError> {
        Ok(std::str::from_utf8(binary)?.to_string())
    }
}

impl<'a> FromSql<'a> for i32 {
    fn from_text(text: &'a [u8]) -> Result<Self, BoxError> {
        Ok(std::str::from_utf8(text)?.parse::<i32>()?)
    }

    fn from_binary(binary: &'a [u8]) -> Result<Self, BoxError> {
        Ok(i32::from_le_bytes(binary.try_into()?))
    }
}
