//! Server-to-client messages.

use std::collections::HashMap;

use crate::util::{CodecError, DecodeError, Reader};

/// The type of server-to-client messages.
///
/// It can be converted to and from [`u8`] values.
#[derive(Debug, Clone)]
pub enum Message {
    /// A message regarding authentication.
    Authentication(Authentication),
    /// A message indicating that an error occurred.
    Error(Error),
    /// A message indicating that a parameter status has changed.
    ParameterStatus(ParameterStatus),
    /// A message indicating that the server is ready for a new query.
    ReadyForQuery,
}

/// The different types of authentication responses.
#[derive(Debug, Clone)]
pub enum Authentication {
    /// The authentication was successful.
    Ok,
    /// The server requested SASL authentication using one of the
    /// mechanisms specified in the list.
    Sasl(Vec<String>),
}

/// A response indicating that an error occurred.
#[derive(Debug, Clone)]
pub struct Error {
    fields: HashMap<u8, String>,
}

/// A message indicating that a parameter status has changed.
#[derive(Debug, Clone)]
pub struct ParameterStatus {
    /// The name of the parameter that changed.
    name: String,
    /// The new value of the parameter.
    value: String,
}

impl<'a> TryFrom<Reader<'a>> for Message {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, <Self as TryFrom<Reader<'a>>>::Error> {
        // The first byte is always the message type.
        let msg_type = match reader.read_u8()? {
            b'R' => Message::ReadyForQuery,
            b'K' => Message::Authentication(Authentication::try_from(reader)?),
            b'E' => Message::Error(Error::try_from(reader)?),
            b'S' => Message::ParameterStatus(ParameterStatus::try_from(reader)?),
            otherwise => {
                return Err(DecodeError::UnexpectedValue(format!(
                    "unknown message type: `{}`, or byte value `{}`",
                    otherwise as char, otherwise
                ))
                .into())
            }
        };

        Ok(msg_type)
    }
}

impl<'a> TryFrom<Reader<'a>> for Authentication {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, Self::Error> {
        // The first 4 bytes are always the authentication response type.
        let message_type = reader.read_i32()?;

        // Now we match on that type to figure out what to do next.
        match message_type {
            // Authentication was successful.
            0 => {
                reader.finish()?;
                Ok(Authentication::Ok)
            }
            // SASL authentication.
            10 => {
                // Read the list of mechanisms (C strings).
                let mut mechanisms = Vec::new();

                // The list is terminated by a zero byte.
                while reader.peek_u8()? != 0 {
                    mechanisms.push(reader.read_cstring()?.to_owned());
                }

                Ok(Authentication::Sasl(mechanisms))
            }
            // Unknown authentication response type.
            otherwise => Err(DecodeError::UnexpectedValue(format!(
                "unknown authentication response type: `{}`",
                otherwise
            ))
            .into()),
        }
    }
}

impl<'a> TryFrom<Reader<'a>> for Error {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, Self::Error> {
        // Ignore the length field.
        reader.skip(4)?;

        let mut fields = HashMap::new();

        // Read the fields and values
        while reader.peek_u8()? != 0 {
            let field = reader.read_u8()?;
            let value = reader.read_cstring()?.to_owned();

            fields.insert(field, value);
        }

        Ok(Error { fields })
    }
}

impl<'a> TryFrom<Reader<'a>> for ParameterStatus {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, Self::Error> {
        // Ignore the length field.
        reader.skip(4)?;

        // Read the name and value of the parameter.
        let name = reader.read_cstring()?.to_owned();
        let value = reader.read_cstring()?.to_owned();

        Ok(ParameterStatus { name, value })
    }
}
