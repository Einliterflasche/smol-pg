use std::fmt::Display;

use crate::{Error, Options, PROTOCOL_VERSION};

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct Message {
    pub type_: u8,
    pub len: u32,
    /// This buffer contains the complete message as sent
    /// by the server. This includes the bytes for `type`
    /// and `len`.
    pub buffer: Vec<u8>,
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct RawMessage {
    pub type_: Option<u8>,
    pub len: u32,
    pub buffer: Vec<u8>,
}

#[derive(PartialEq, Eq, Debug, Hash, Clone)]
pub struct Payload(Vec<u8>);

#[derive(PartialEq, Eq, Debug, Hash, Clone, Copy)]
#[repr(u8)]
pub enum BackendMessageType {
    Authentication = b'R',
    CancellationKeyData = b'K',
    BindComplete = b'2',
    CloseComplete = b'3',
    CommandComplete = b'C',
    CopyData = b'd',
    CopyDone = b'c',
    CopyIn = b'G',
    CopyOut = b'H',
    CopyBoth = b'W',
    DataRow = b'D',
    EmptyQuery = b'I',
    Error = b'E',
    FunctionCall = b'V',
    NegotiateProtocolVersion = b'v',
    NoData = b'n',
    Notice = b'N',
    Notification = b'A',
    ParameterDescription = b't',
    ParameterStatus = b'S',
    ParseComplete = b'1',
    PortalSuspended = b's',
    ReadyForQuery = b'Z',
    RowDescription = b'T',
}

pub mod frontend {
    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    pub struct QueryMessage(pub String);
}

pub mod backend {
    use crate::Error;

    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    pub struct ErrorResponse(Vec<(u8, String)>);
    #[derive(Debug, PartialEq, Eq, Hash, Clone)]
    pub struct DataRow(Vec<Vec<u8>>);

    impl TryFrom<&[u8]> for ErrorResponse {
        type Error = Error;

        fn try_from(buffer: &[u8]) -> Result<Self, Self::Error> {
            if buffer.len() < 6 {
                return Err(Error::InvalidMessage(
                    super::BackendMessageType::Error,
                    "message too short",
                ));
            }

            let mut fields = Vec::new();

            for part in buffer[5..].split(|byte| *byte == 0).filter(|i| i.len() > 0) {
                let type_ = part[0];
                let value = String::from_utf8(part[1..].to_vec()).map_err(|_| {
                    Error::InvalidMessage(
                        super::BackendMessageType::Error,
                        "string in message not utf-8",
                    )
                })?;

                fields.push((type_, value));
            }

            Ok(ErrorResponse(fields))
        }
    }

    impl TryFrom<&[u8]> for DataRow {
        type Error = Error;

        fn try_from(buffer: &[u8]) -> Result<Self, Self::Error> {
            if buffer.len() < 7 {
                return Err(Error::InvalidMessage(
                    super::BackendMessageType::DataRow,
                    "message too short",
                ));
            }

            if buffer[0] != b'D' {
                return Err(Error::InvalidMessage(
                    super::BackendMessageType::CopyData,
                    "wrong message type",
                ));
            }
            // let supposed_num_rows = u16::from_be_bytes([buffer[5], buffer[6]]);
            let mut rows = Vec::new();

            let mut curr_row_start: usize = 7;

            while curr_row_start + 1 < buffer.len() {
                // 0th and 1st byte of each row give the row length
                let supposed_row_len =
                    u16::from_be_bytes([buffer[curr_row_start], buffer[curr_row_start + 1]]);

                // supposed_row_len doesn't include itself -> + 2
                let supposed_row_end = curr_row_start + supposed_row_len as usize + 2;

                if supposed_row_end >= buffer.len() {
                    return Err(Error::InvalidMessage(
                        super::BackendMessageType::DataRow,
                        "supposed row longer than buffer",
                    ));
                }

                rows.push(buffer[curr_row_start + 2..=supposed_row_end].to_vec());
                curr_row_start = supposed_row_end + 1;
            }

            Ok(DataRow(rows))
        }
    }
}

pub fn create_query_message(query: &str) -> Vec<u8> {
    let mut buffer = prepend_type_and_length(b'Q', query.as_bytes());
    buffer.push(b'\0');

    buffer
}

pub fn create_startup_message(options: &Options) -> Vec<u8> {
    let mut buffer: Vec<u8> = Vec::new();

    // set protocol version
    buffer.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());

    // set client_encoding so the server knows we use utf-8
    // (\0 terminates a string)
    buffer.extend_from_slice("client_encoding\0UTF-8\0".as_bytes());

    // set user == username
    buffer.extend_from_slice("user\0".as_bytes());
    buffer.extend_from_slice(options.user_name.as_bytes());
    // one \0 to end `user_name` string
    buffer.push(0);

    if let Some(database) = &options.database {
        buffer.extend_from_slice("database\0".as_bytes());
        buffer.extend_from_slice(database.as_bytes());
        buffer.push(0);
    }

    // one last null byte to signal the end of the options
    buffer.push(0);

    // at this point the buffer looks like this (excluding the database option):
    // "{PROTOCOL_VERSION}name\0{user_name}\0\0"

    // now we preprend the length of the whole message
    // so the server knows how much to read
    preprend_length(&buffer)
}

/// Return a vector with the same contents, execpt that
/// the first 4 bytes represent the length of the new vector.
/// The length includes the bytes used to represent the length itself.
///
/// Will panic if `buffer.len() + 4` exceeds `i32::MAX`.
pub fn preprend_length(buffer: &[u8]) -> Vec<u8> {
    let mut new_buffer = Vec::new();

    let length_in_bytes = i32::try_from(buffer.len() + 4)
        .expect("length overflowed i32")
        .to_be_bytes();

    new_buffer.extend_from_slice(&length_in_bytes);
    new_buffer.extend_from_slice(buffer);

    new_buffer
}

pub fn prepend_type_and_length(type_: u8, buffer: &[u8]) -> Vec<u8> {
    let mut new_buffer = vec![type_];
    let length_in_bytes = i32::try_from(buffer.len() + 5)
        .expect("length overflowed i32")
        .to_be_bytes();
    new_buffer.extend_from_slice(&length_in_bytes);
    new_buffer.extend_from_slice(buffer);

    new_buffer
}

impl AsRef<[u8]> for Message {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

impl AsMut<[u8]> for Message {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

impl AsRef<[u8]> for RawMessage {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}

impl AsMut<[u8]> for RawMessage {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

impl TryFrom<u8> for BackendMessageType {
    type Error = crate::Error;

    fn try_from(value: u8) -> Result<Self, Error> {
        use BackendMessageType as T;

        Ok(match value {
            b'R' => T::Authentication,
            b'K' => T::CancellationKeyData,
            b'2' => T::BindComplete,
            b'3' => T::CloseComplete,
            b'C' => T::CommandComplete,
            b'd' => T::CopyData,
            b'c' => T::CopyDone,
            b'G' => T::CopyIn,
            b'H' => T::CopyOut,
            b'W' => T::CopyBoth,
            b'D' => T::DataRow,
            b'I' => T::EmptyQuery,
            b'E' => T::Error,
            b'V' => T::FunctionCall,
            b'v' => T::NegotiateProtocolVersion,
            b'n' => T::NoData,
            b'N' => T::Notice,
            b'A' => T::Notification,
            b't' => T::ParameterDescription,
            b'S' => T::ParameterStatus,
            b'1' => T::ParseComplete,
            b's' => T::PortalSuspended,
            b'Z' => T::ReadyForQuery,
            b'T' => T::RowDescription,
            _ => return Err(Error::UnknownMessage(value)),
        })
    }
}

impl Display for BackendMessageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#?}", self)
    }
}
