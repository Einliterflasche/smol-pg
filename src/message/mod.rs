use std::collections::HashMap;

use thiserror_lite::err_enum;
use macro_rules_attribute::apply;

use crate::codec::{DecodeError, EncodeError, Reader, Writer};

/// Errors that can occur using this crate.
#[apply(err_enum)]
#[derive(Debug, Clone)]
pub enum Error {
    #[error("failed to decode message")]
    DecodeError(DecodeError),
    #[error("failed to encode message")]
    EncodeError(EncodeError),
    #[error("unexpected message type")]
    UnexpectedMessageType,
}

/// A message sent by the server.
pub trait ServerMessage: Sized {
    fn decode(bytes: &[u8]) -> Result<Self, Error>;
}

/// A message sent by the client.
pub trait ClientMessage: Sized {
    fn encode(&self) -> Vec<u8>;
}

/// A message sent by the client to initiate a connection.
pub struct Startup {
    pub options: HashMap<String, String>
}

impl Startup {
    pub fn new(username: impl Into<String>, database: impl Into<Option<String>>) -> Self {
        let mut options = HashMap::new();

        // Insert options into the map
        options.insert("user".to_string(), username.into());
        if let Some(database) = database.into() {
            options.insert("database".to_string(), database);
        }

        Self {
            options
        }
    }
}

impl ClientMessage for Startup {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Writer::new();

        // A placeholder for the message length, which we will update later
        buf.write_i32(0);
        buf.write_i32(crate::PROTOCOL_VERSION);

        // Write the connection options
        for (key, value) in self.options.iter() {
            buf.write_cstr(key);
            buf.write_cstr(value);
        }

        // To signal the end of the message
        buf.write_null_byte();

        // Update the message length
        buf
            .overwrite_message_length()
            .expect("buffer to be at least 4 bytes");

        buf.into_vec()
    }
}

/// A message sent by the server to indicate the
/// supported authentication methods.
#[derive(Debug)]
pub enum Authentication {
    Ok,
    CleartextPassword,
}

impl ServerMessage for Authentication {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes);

        let message_type = reader.read_this_u8(b'R')?;
        let _message_length = reader.read_i32()? as usize;
        let auth_type = reader.read_i32()?;


        match (message_type, auth_type) {
            (b'R', 0) => Ok(Authentication::Ok),
            (b'R', 3) => Ok(Authentication::CleartextPassword),
            (b'R', _) => unimplemented!("unsupported authentication method"),
            (_, _) => Err(Error::UnexpectedMessageType),
        }
    }
}

/// A message sent by the server to indicate an error.
#[derive(Debug, Clone)]
pub struct ErrorResponse {
    /// The fields of the error message.
    pub fields: HashMap<ErrorType, String>
}

/// The possible error types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ErrorType {
    Severity,
    Code,
    Message,
    Detail,
}

impl ServerMessage for ErrorResponse {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes);

        let _message_type = reader.read_u8()?;
        let _message_length = reader.read_i32()? as usize;

        let mut fields = HashMap::new();

        loop {
            let field_type = reader.read_u8()?;

            // Terminator null byte
            let field_type = match field_type {
                // Terminator null byte
                0 => break,
                b'S' => ErrorType::Severity,
                b'C' => ErrorType::Code,
                b'M' => ErrorType::Message,
                b'D' => ErrorType::Detail,
                // Per specification, we should ignore any unknown field types
                _ => continue,
            };

            let field_value = reader.read_cstr()?;

            fields.insert(field_type, field_value.to_string());
        }

        Ok(ErrorResponse { fields })
    }
}

/// A message sent by the server to indicate that it is ready for a new query.
#[derive(Debug, Clone)]
pub struct ReadyForQuery {
    /// Whether this connection is idle, in a transaction,
    /// or the transaction has failed.
    pub transaction_status: TransactionStatus
}

/// Whether this connection is idle, in a transaction,
/// or the transaction has failed.
#[derive(Debug, Clone)]
pub enum TransactionStatus {
    Idle,
    InTransaction,
    Failed,
}

impl ServerMessage for ReadyForQuery {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes);

        let _message_type = reader.read_this_u8(b'Z')?;
        let _message_length = reader.read_i32()? as usize;

        let transaction_status = reader.read_u8()?;
        let transaction_status = match transaction_status {
            b'I' => TransactionStatus::Idle,
            b'T' => TransactionStatus::InTransaction,
            b'E' => TransactionStatus::Failed,
            _ => return Err(Error::DecodeError(DecodeError::UnexpectedValue)),
        };

        Ok(ReadyForQuery { transaction_status })
    }
}

/// A message sent by the server including the server's
/// configuration parameters.
#[derive(Debug, Clone)]
pub struct ParameterStatus {
    pub name: String,
    pub value: String,
}

impl ServerMessage for ParameterStatus {
    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes);

        let _message_type = reader.read_this_u8(b'S')?;
        let _message_length = reader.read_i32()? as usize;

        let name = reader.read_cstr()?.to_string();
        let value = reader.read_cstr()?.to_string();

        Ok(ParameterStatus { name, value })
    }
}

impl From<DecodeError> for Error {
    fn from(value: DecodeError) -> Self {
        Error::DecodeError(value)
    }
}

impl From<EncodeError> for Error {
    fn from(value: EncodeError) -> Self {
        Error::EncodeError(value)
    }
}
