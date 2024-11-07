use std::ops::Deref;

use macro_rules_attribute::apply;
use thiserror_lite::err_enum;

#[apply(err_enum)]
#[derive(Debug, Clone)]
pub enum DecodeError {
    #[error("buffer too short to contain expected data")]
    BufTooShort,
    #[error("encountered non-UTF-8 string")]
    NonUtf8String,
    #[error("encountered unexpected value")]
    UnexpectedValue,
}

#[apply(err_enum)]
#[derive(Debug, Clone)]
pub enum EncodeError {
    #[error("buffer too short to contain message length")]
    BufTooShort,
}

/// For orderly reading from a byte buffer.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

/// For orderly writing to a byte buffer.
#[derive(Debug, Clone)]
pub struct Writer {
    buffer: Vec<u8>,
}

impl<'a> Reader<'a> {
    /// Create a new reader from a byte slice.
    pub fn new(bytes: &'a [u8]) -> Self {
        Reader { bytes, position: 0 }
    }

    /// Read a u8 from the buffer.
    pub fn read_u8(&mut self) -> Result<u8, DecodeError> {
        if self.position >= self.bytes.len() {
            return Err(DecodeError::BufTooShort);
        }

        let byte = self.bytes[self.position];
        self.position += 1;
        Ok(byte)
    }

    /// Read a u8 from the buffer and check if it matches the expected value.
    pub fn read_this_u8(&mut self, expected: u8) -> Result<u8, DecodeError> {
        // Read the byte
        let byte = self.read_u8()?;
        // Check if it matches the expected value
        if byte != expected {
            return Err(DecodeError::UnexpectedValue);
        }

        Ok(byte)
    }

    /// Read a 32-bit integer (little-endian) from the buffer.
    pub fn read_i32(&mut self) -> Result<i32, DecodeError> {
        if self.position + 4 > self.bytes.len() {
            return Err(DecodeError::BufTooShort);
        }

        let bytes = [
            self.bytes[self.position],
            self.bytes[self.position + 1],
            self.bytes[self.position + 2],
            self.bytes[self.position + 3],
        ];
        
        self.position += 4;

        Ok(i32::from_le_bytes(bytes))
    }

    /// Read a 32-bit integer (little-endian) from the buffer and check if it matches the expected value.
    pub fn read_this_i32(&mut self, expected: i32) -> Result<i32, DecodeError> {
        // Read the value
        let value = self.read_i32()?;
        // Check if it matches the expected value
        if value != expected {
            return Err(DecodeError::UnexpectedValue);
        }

        Ok(value)
    }

    /// Read a null-terminated C string from the buffer.
    pub fn read_cstr(&mut self) -> Result<&str, DecodeError> {
        // Find the null terminator
        let null_index = self
            .bytes[self.position..]
            .iter()
            .position(|&b| b == 0)
            .ok_or(DecodeError::BufTooShort)?;
        // Make sure it's valid UTF-8
        let cstr = std::str::from_utf8(&self.bytes[self.position..self.position + null_index])
            .map_err(|_| DecodeError::NonUtf8String)?;
        // Move past the null terminator
        self.position += cstr.len() + 1;
        
        Ok(cstr)
    }

    /// Read a byte from the buffer and check if it is null.
    pub fn read_null_byte(&mut self) -> Result<(), DecodeError> {
        // Read the byte
        let value = self.read_u8()?;
        // Check if it matches the expected value
        if value != 0 {
            return Err(DecodeError::UnexpectedValue);
        }

        Ok(())
    }
}

impl Writer {
    /// Create a new writer.
    pub fn new() -> Self {
        Writer { buffer: Vec::new() }
    }

    /// Create a new writer with a given capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Writer { buffer: Vec::with_capacity(capacity) }
    }

    /// Convert the writer to a byte buffer to finalize the message.
    pub fn into_vec(self) -> Vec<u8> {
        self.buffer
    }

    /// Write a u8 to the buffer.
    pub fn write_u8(&mut self, value: u8) {
        self.buffer.push(value);
    }

    /// Write a 32-bit integer (little-endian) to the buffer.
    pub fn write_i32(&mut self, value: i32) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    /// Write a null-terminated C string to the buffer.
    pub fn write_cstr(&mut self, value: &str) {
        self.buffer.extend_from_slice(value.as_bytes());
        self.buffer.push(0);
    }

    /// Write a null byte to the buffer.
    pub fn write_null_byte(&mut self) {
        self.buffer.push(0);
    }

    /// Overwrite the message length in the first 4 byts of the buffer.
    pub fn overwrite_message_length(&mut self) -> Result<(), EncodeError> {
        if self.buffer.len() < 4 {
            return Err(EncodeError::BufTooShort);
        }

        let message_length = self.buffer.len() as i32;
        self.buffer[0..4].copy_from_slice(&message_length.to_le_bytes());

        Ok(())
    }
}

impl From<Writer> for Vec<u8> {
    fn from(writer: Writer) -> Self {
        writer.buffer
    }
}

impl Deref for Writer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl AsRef<[u8]> for Writer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}
