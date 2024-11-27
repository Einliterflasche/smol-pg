#![no_std]
extern crate alloc;

use alloc::string::{String, FromUtf8Error};
use alloc::vec::Vec;

use macro_rules_attribute::apply;
use thiserror_lite::err_enum;

/// Errors that can occur when encoding or decoding a value or message.
#[apply(err_enum)]
#[derive(Debug, Clone)]
pub enum CodecError {
    #[error(transparent)]
    Decode(DecodeError),
    #[error(transparent)]
    Encode(EncodeError),
}

/// Errors that can occur when decoding a value or message from a byte buffer.
#[apply(err_enum)]
#[derive(Debug, Clone)]
pub enum DecodeError {
    #[error("unexpected EOF, buffer shorter than expected")]
    UnexpectedEof,
    #[error("unexpected byte, expected {expected}, got {got}")]
    UnexpectedByte { expected: u8, got: u8 },
    #[error(transparent)]
    Utf8Error(FromUtf8Error),
}

/// Errors that can occur when encoding a value or message to a byte buffer.
#[apply(err_enum)]
#[derive(Debug, Clone)]
pub enum EncodeError {
    #[error("attempted to backtrack too many bytes")]
    BufferTooShort,
}

/// A helper struct to read values from a byte buffer
/// in a safe manner.
pub struct Reader<'a> {
    buffer: &'a [u8],
    position: usize,
}

/// A helper struct to write values to a byte buffer
/// in a safe manner.
pub struct Writer {
    buffer: Vec<u8>,
}

impl<'a> Reader<'a> {
    /// Create a new reader over a given buffer.
    pub fn new(buffer: &'a [u8]) -> Self {
        Self { buffer, position: 0 }
    }

    /// Skip the next `n` bytes in the buffer.
    pub fn skip(&mut self, n: usize) -> Result<(), DecodeError> {
        if self.position + n > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        self.position += n;
        Ok(())
    }

    /// Returns an error if the buffer does not contain at least `n` more bytes.
    pub fn ensure_remaining(&self, n: usize) -> Result<(), DecodeError> {
        if self.position + n > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        Ok(())
    }

    /// Returns an error if the buffer does not contain exactly `n` more bytes.
    pub fn ensure_remaining_exact(&self, n: usize) -> Result<(), DecodeError> {
        if self.position + n != self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        Ok(())
    }

    /// Peek at the next byte in the buffer without advancing the position.
    pub fn peek_u8(&self) -> Result<u8, DecodeError> {
        if self.position + 1 > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        Ok(self.buffer[self.position])
    }

    /// Peek at the next `N` bytes in the buffer without advancing the position.
    pub fn peek_bytes<const N: usize>(&self) -> Result<&'a [u8; N], DecodeError> {
        if self.position + N > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        Ok(self.buffer[self.position..self.position + N]
            .try_into()
            .expect("slice to be `N` bytes long"))
    }

    /// Returns an error if the next byte in the buffer is not equal to the
    /// expected value.
    pub fn expect_u8(&self, value: u8) -> Result<(), DecodeError> {
        if self.peek_u8()? != value {
            return Err(DecodeError::UnexpectedEof);
        }

        Ok(())
    }

    /// Read a single byte from the buffer.
    pub fn read_u8(&mut self) -> Result<u8, DecodeError> {
        if self.position + 1 > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        let value = self.buffer[self.position];
        self.position += 1;
        Ok(value)
    }

    /// Read a slice of `N` bytes from the buffer.
    pub fn read_bytes<const N: usize>(&mut self) -> Result<&'a [u8; N], DecodeError> {
        if self.position + N > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        let value = self.buffer[self.position..self.position + N]
            .try_into()
            .expect("slice to be `N` bytes long");
        self.position += N;
        Ok(value)
    }

    /// Read the remaining bytes in the buffer.
    pub fn read_remaining_bytes(&mut self) -> Result<&'a [u8], DecodeError> {
        let value = &self.buffer[self.position..];
        self.position = self.buffer.len();
        Ok(value)
    }

    /// Read a 32-bit integer from the buffer in big-endian (network) order.
    pub fn read_i32(&mut self) -> Result<i32, DecodeError> {
        let bytes = self.read_bytes::<4>()?;
        Ok(i32::from_be_bytes(*bytes))
    }

    /// Read a C string (null-terminated) from the buffer.
    pub fn read_cstring(&mut self) -> Result<&'a str, DecodeError> {
        // Find the position of the null terminator
        let null_position = self.buffer.iter().position(|byte| *byte == 0);

        let Some(null_position) = null_position else {
            return Err(DecodeError::UnexpectedEof);
        };

        // Read the string from the buffer
        let string = std::str::from_utf8(&self.buffer[self.position..null_position])
            .map_err(|_| DecodeError::UnexpectedEof)?;

        self.position += string.len() + 1; // +1 for the null terminator
        Ok(string)
    }

    /// Returns an error if the reader has not read all the bytes in the buffer.
    pub fn finish(self) -> Result<(), DecodeError> {
        if self.position != self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        Ok(())
    }
}

impl Writer {
    /// Create a new writer.
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
        }
    }

    /// Write a single byte to the buffer.
    pub fn write_u8(&mut self, value: u8) {
        self.buffer.push(value);
    }

    /// Write a slice of bytes to the buffer.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// Write a 32-bit integer to the buffer in big-endian (network) order.
    pub fn write_i32(&mut self, value: i32) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    /// Write a C string (null-terminated) to the buffer.
    pub fn write_cstring(&mut self, string: &str) {
        self.write_bytes(string.as_bytes());
        self.write_u8(0);
    }

    /// Backtrack the last `n` bytes in the buffer, but at most the length of
    /// the buffer.
    pub fn backtrack(&mut self, n: usize) {
        let n = n.max(self.buffer.len());
        self.buffer.truncate(n);
    }

    /// Try to backtrack exactly the last `n` bytes in the buffer.
    pub fn try_backtrack(&mut self, n: usize) -> Result<(), DecodeError> {
        if n > self.buffer.len() {
            return Err(DecodeError::UnexpectedEof);
        }

        self.buffer.truncate(n);
        Ok(())
    }

    /// Finish the writer and return the underlying buffer.
    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }
}
