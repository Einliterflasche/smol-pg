//! Server-to-client messages.

use std::{collections::HashMap, ops::Index};

use crate::util::{BoxError, CodecError, DecodeError, Reader};

use super::parsing::FromSql;

/// The type of server-to-client messages.
///
/// It can be converted to and from [`u8`] values.
#[derive(Debug, Clone)]
pub enum Message {
    /// A message regarding authentication.
    Authentication(Authentication),
    /// An error occured.
    Error(Error),
    /// A notice in relation to a request.
    Notice(Notice),
    /// A parameter status has changed.
    ParameterStatus(ParameterStatus),
    /// A key necessary for issuing cancel requests.
    KeyData(KeyData),
    /// The server is ready for a new query.
    ReadyForQuery,
    /// A response to an empty query.
    /// This is issued instead of `CommandComplete` for empty queries.
    EmptyQuery,
    /// A command completed successfully.
    CommandComplete(CommandComplete),
    /// Information about the columns of a result set.
    RowDescription(RowDescription),
    /// A row of data from a result set.
    DataRow(DataRow),
}

/// The different types of authentication responses.
#[derive(Debug, Clone)]
pub enum Authentication {
    /// The authentication was successful.
    Ok,
    /// The server requested SASL authentication using one of the
    /// mechanisms specified in the list.
    Sasl(Vec<String>),
    /// The server provided data for continuing SASL authentication.
    SaslContinue(Vec<u8>),
    /// SASL authentication is complete.
    SaslFinal(Vec<u8>),
}

/// A response indicating that an error occurred.
#[derive(Debug, Clone)]
pub struct Error {
    fields: HashMap<u8, String>,
}

/// A message indicating a notice in relation to a request.
#[derive(Debug, Clone)]
pub struct Notice {
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

/// A message including a key necessary for issuing cancel requests.
#[derive(Debug, Clone)]
pub struct KeyData {
    /// The process ID of the server process that generated the key.
    process_id: i32,
    /// The secret key necessary for issuing cancel requests.
    secret_key: i32,
}

/// A response indicating that a command completed successfully.
#[derive(Debug, Clone)]
pub struct CommandComplete {
    /// The command tag of the completed command.
    tag: String,
}

/// The result of a (select-like) query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The row description of the query.
    row_description: RowDescription,
    /// The data rows of the query.
    data_rows: Vec<DataRow>,
}

/// Information about the columns of a result set.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowDescription {
    pub fields: Vec<FieldDescription>,
}

/// A row containing a series of data cells representing a row in a [`QueryResult`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataRow {
    /// The different data fields in this row.
    pub(crate) fields: Vec<Data>,
}

/// A field in a data row.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Data(Vec<u8>);

/// Information about a field in a result set.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldDescription {
    /// The name of the field.
    pub name: String,
    /// If the field is a column of a table, the object ID
    /// of the table containing the field.
    pub table_oid: Option<i32>,
    /// If the field is a column of a table, the attribute number of the column.
    pub attribute_number: Option<i16>,
    /// The object ID of the data type of the field.
    pub data_type_oid: i32,
    /// The length of the field in bytes.
    pub data_type_size: i16,
    /// The type modifier of the field.
    pub data_type_modifier: i32,
    /// The format code of the field.
    ///
    /// This is `0` for text format and `1` for binary format.
    pub format_code: FormatCode,
}

/// The format code of a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatCode {
    /// Text format.
    Text,
    /// Binary format.
    Binary,
}

impl<'a> TryFrom<Reader<'a>> for Message {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, <Self as TryFrom<Reader<'a>>>::Error> {
        // The first byte is always the message type.
        let msg_type = match reader.read_u8()? {
            b'Z' => Message::ReadyForQuery,
            b'R' => Message::Authentication(Authentication::try_from(reader)?),
            b'E' => Message::Error(Error::try_from(reader)?),
            b'S' => Message::ParameterStatus(ParameterStatus::try_from(reader)?),
            b'K' => Message::KeyData(KeyData::try_from(reader)?),
            b'I' => Message::EmptyQuery,
            b'C' => Message::CommandComplete(CommandComplete::try_from(reader)?),
            b'N' => Message::Notice(Notice::try_from(reader)?),
            b'T' => Message::RowDescription(RowDescription::try_from(reader)?),
            b'D' => Message::DataRow(DataRow::try_from(reader)?),
            otherwise => Err(DecodeError::UnexpectedValue(format!(
                "unknown message type: `{}`, or byte value `{}`",
                otherwise as char, otherwise
            )))?,
        };

        Ok(msg_type)
    }
}

impl<'a> TryFrom<Reader<'a>> for Authentication {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, Self::Error> {
        // Ignore the length
        reader.skip(4)?;

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
            // SASL authentication is continuing.
            11 => {
                let data = reader.read_remaining_bytes()?;
                Ok(Authentication::SaslContinue(data.to_owned()))
            }
            // SASL authentication is complete.
            12 => {
                let data = reader.read_remaining_bytes()?;
                Ok(Authentication::SaslFinal(data.to_owned()))
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

impl<'a> TryFrom<Reader<'a>> for Notice {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, Self::Error> {
        // Ignore the length field.
        reader.skip(4)?;

        let mut fields = HashMap::new();

        while reader.peek_u8()? != 0 {
            let field = reader.read_u8()?;
            let value = reader.read_cstring()?.to_owned();

            fields.insert(field, value);
        }

        Ok(Notice { fields })
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

impl<'a> TryFrom<Reader<'a>> for KeyData {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, Self::Error> {
        // Ignore the length field.
        reader.skip(4)?;

        // Read the process ID and secret key.
        let process_id = reader.read_i32()?;
        let secret_key = reader.read_i32()?;

        Ok(KeyData {
            process_id,
            secret_key,
        })
    }
}

impl<'a> TryFrom<Reader<'a>> for CommandComplete {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, Self::Error> {
        // Ignore the length field.
        reader.skip(4)?;

        // Read the command tag.
        let tag = reader.read_cstring()?.to_owned();

        Ok(CommandComplete { tag })
    }
}

impl<'a> TryFrom<Reader<'a>> for RowDescription {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, Self::Error> {
        // Ignore the length field.
        reader.skip(4)?;

        // Read the number of fields.
        let field_count = reader.read_i16()?;

        if field_count < 0 {
            return Err(DecodeError::UnexpectedValue(
                "negative number of fields in row description".to_string(),
            )
            .into());
        }

        let mut fields = Vec::with_capacity(field_count as usize);

        for _ in 0..field_count {
            fields.push(FieldDescription::try_from(&mut reader)?);
        }

        Ok(RowDescription { fields })
    }
}

impl<'a> TryFrom<Reader<'a>> for DataRow {
    type Error = CodecError;

    fn try_from(mut reader: Reader<'a>) -> Result<Self, Self::Error> {
        // Ignore the length field.
        reader.skip(4)?;

        let n = reader.read_i16()?;
        let n = match n {
            0.. => n as usize,
            otherwise => {
                return Err(DecodeError::UnexpectedValue(format!(
                    "negative number of fields in data row: `{}`",
                    otherwise
                ))
                .into());
            }
        };

        let mut fields = Vec::with_capacity(n);

        for _ in 0..n {
            let field_size = reader.read_i32()?;
            let field_size = match field_size {
                1.. => field_size as usize,
                _ => {
                    fields.push(Data(Vec::new()));
                    continue;
                }
            };

            let bytes = reader.read_bytes(field_size)?.to_owned();

            fields.push(Data(bytes));
        }

        Ok(DataRow { fields })
    }
}

impl<'a> TryFrom<&mut Reader<'a>> for FieldDescription {
    type Error = CodecError;

    fn try_from(reader: &mut Reader<'a>) -> Result<Self, Self::Error> {
        // Read the values for the field.
        let name = reader.read_cstring()?.to_owned();
        let table_oid = reader.read_i32()?;
        let attribute_number = reader.read_i16()?;
        let data_type_oid = reader.read_i32()?;
        let data_type_size = reader.read_i16()?;
        let data_type_modifier = reader.read_i32()?;
        let format_code = reader.read_i16()?;

        // Convert some values
        let table_oid = if table_oid == 0 {
            None
        } else {
            Some(table_oid)
        };

        let attribute_number = if attribute_number == 0 {
            None
        } else {
            Some(attribute_number)
        };

        let format_code = match format_code {
            0 => FormatCode::Text,
            1 => FormatCode::Binary,
            otherwise => {
                return Err(DecodeError::UnexpectedValue(format!(
                    "unknown format code: `{}`",
                    otherwise
                ))
                .into());
            }
        };

        Ok(FieldDescription {
            name,
            table_oid,
            attribute_number,
            data_type_oid,
            data_type_size,
            data_type_modifier,
            format_code,
        })
    }
}

impl RowDescription {
    /// Get the index of a field by name.
    pub(crate) fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }
}

impl<'a> Data {
    /// Parse a value from text.
    pub(crate) fn parse_text<T: FromSql<'a>>(&'a self) -> Result<T, BoxError> {
        T::from_text(&self.0)
    }

    /// Parse a value from binary.
    pub(crate) fn parse_binary<T: FromSql<'a>>(&'a self) -> Result<T, BoxError> {
        T::from_binary(&self.0)
    }
}
