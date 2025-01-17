//! This module contains the networking part of the connection.
//! Here, we write the messages to and read them from the buffer and handle them.

use std::{collections::VecDeque, fmt::Display, net::IpAddr, sync::Arc};

use async_net::TcpStream;
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use macro_rules_attribute::apply;
use thiserror_lite::err_enum;

use crate::{
    protocol::message::{
        client,
        parsing::FromSql,
        server::{self, Data, FormatCode, RowDescription},
    },
    util::{self, BoxError, DecodeError},
    Error,
};

/// Errors that can occur when using the protocol.
#[allow(missing_docs)]
#[derive(Debug)]
#[apply(err_enum)]
pub enum ProtocolError {
    #[error("missing row description")]
    MissingRowDescription,
}

/// Attempted and failed to access a field of a row because it doesn't exist.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldNotFound(String);

/// A connection to a PostgreSQL server.
///
/// This struct is generic over all transport layers
/// that implement the required traits.
pub struct Connection {
    /// The bi-directional stream that is the transport layer.
    stream: TcpStream,
    /// Here we buffer responses from the server until we handle them.
    response_buffer: VecDeque<server::Message>,
    /// Whether we are ready to send a query to the server.
    ready_to_query: bool,
    /// The key data from the backend we need to cancel queries.
    key_data: Option<server::KeyData>,
}

/// A row in a result set.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Row {
    /// The reference to the metadata of this row.
    metadata: Arc<RowDescription>,
    /// The fields in this row.
    fields: Vec<Data>,
}

impl Connection {
    /// Open and return a new connection to the PostgreSQL server
    /// at the given address and port.
    ///
    /// Uses port `5432` if none is provided.
    pub async fn create(address: IpAddr, port: Option<u16>) -> Result<Self, Error> {
        let port = port.unwrap_or(crate::POSTGRES_DEFAULT_PORT);

        // Create the TCP connection
        let stream = TcpStream::connect((address, port))
            .await
            .map_err(Error::NetworkError)?;

        // Create the connection
        let mut conn = Self::new(stream);

        // Startup routine
        let startup_message = client::Startup::new("postgres".to_string(), None, None);
        conn.send_message(&startup_message).await?;

        // Buffer all messages until we are ready to query
        loop {
            let response = conn.read_message().await?;

            tracing::debug!(response=?&response, "Received message from server");

            // We won't handle any messages until we are ready to query
            match response {
                server::Message::ReadyForQuery => {
                    conn.ready_to_query = true;
                    break;
                }
                otherwise => conn.response_buffer.push_back(otherwise),
            }
        }

        Ok(conn)
    }

    /// Send a query to the server.
    pub async fn query(&mut self, query: &str) -> Result<Vec<Row>, Error> {
        let query_message = client::Query::new(query.to_string());
        self.send_message(&query_message).await?;

        let mut row_description = None;
        let mut data_rows = Vec::new();

        // Read messages until we have a command complete message
        loop {
            let response = self.read_message().await?;

            tracing::debug!(response=?&response, "Received message from server");

            match response {
                // Command complete means we are done reading messages for this query
                server::Message::CommandComplete(command_complete) => {
                    tracing::debug!(command_complete=?command_complete, "Command complete");
                    break;
                }
                // Row description is the header info for the result set
                server::Message::RowDescription(description) => {
                    row_description = Some(description);
                }
                // Data row is a row in the result set
                server::Message::DataRow(data_row) => {
                    tracing::debug!(data_row=?data_row, "Data row");
                    data_rows.push(data_row);
                }
                // Error means something went wrong
                server::Message::Error(error) => {
                    tracing::error!(error=?error, "Query error");
                    panic!("oops");
                }
                // Otherwise, we just buffer this message for later processing
                otherwise => self.response_buffer.push_back(otherwise),
            }
        }

        // We received the complete response, now we can return the rows
        let row_description =
            Arc::new(row_description.ok_or(ProtocolError::MissingRowDescription)?);

        let rows = data_rows
            .into_iter()
            .map(|data_row| Row {
                metadata: row_description.clone(),
                fields: data_row.fields,
            })
            .collect();

        Ok(rows)
    }

    /// Create a new connection from a bi-directional stream.
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            response_buffer: VecDeque::new(),
            ready_to_query: false,
            key_data: None,
        }
    }

    /// Send a message to the server.
    async fn send_message(&mut self, message: impl Into<Vec<u8>>) -> Result<(), Error> {
        // Write the message to the stream
        self.stream
            .write_all(&message.into())
            .await
            .map_err(Error::NetworkError)?;

        // Flush the stream to ensure the message is sent
        self.stream.flush().await.map_err(Error::NetworkError)?;

        Ok(())
    }

    /// Read a message from the stream, appending it to the buffer (resizing it if necessary).
    async fn read_message(&mut self) -> Result<server::Message, Error> {
        // This is how many bytes of header each response has
        const HEADER_LENGTH: usize = 5;

        // Read the message type
        let mut message_type_buf = [0; 1];
        self.stream
            .read_exact(&mut message_type_buf)
            .await
            .map_err(Error::NetworkError)?;

        // Read the message length
        let mut message_length_buf = [0; 4];
        self.stream
            .read_exact(&mut message_length_buf)
            .await
            .map_err(Error::NetworkError)?;

        // Convert message length and sanity check
        let message_length = i32::from_be_bytes(message_length_buf);
        if message_length < 4 {
            return Err(Error::CodecError(
                DecodeError::UnexpectedValue("message length implausibly small".to_string()).into(),
            ));
        }

        // Actual message length is one byte larger since it doesn't include the message type
        let actual_message_length = message_length as usize + 1;

        // Make sure there is enough space in the buffer
        let mut buf = Vec::with_capacity(actual_message_length);

        // Add the message type and length to the buffer
        buf.extend_from_slice(&message_type_buf);
        buf.extend_from_slice(&message_length_buf);

        // Fill the buffer with zeros where the message content will be written
        buf.resize(actual_message_length, 0u8);

        // Read the message content
        self.stream
            .read_exact(&mut buf[HEADER_LENGTH..])
            .await
            .map_err(Error::NetworkError)?;

        // Decode the message
        let message =
            server::Message::try_from(util::Reader::new(&buf)).map_err(Error::CodecError)?;

        Ok(message)
    }

    /// Read a message from the stream now, without waiting for more data,
    /// or return `None` if there are no bytes available to read.
    async fn read_message_now(&mut self) -> Result<Option<server::Message>, Error> {
        tracing::trace!("Checking for available bytes");

        // If there are no bytes available, return `None`
        if !self.has_bytes().await? {
            tracing::trace!("No bytes available to read");
            return Ok(None);
        }

        tracing::trace!("Bytes available, reading message");

        // Otherwise, read the message
        Ok(Some(self.read_message().await?))
    }

    /// Check whether there are any bytes available to read.
    async fn has_bytes(&mut self) -> Result<bool, Error> {
        let mut buf = [0u8; 1];

        // Peek at the first byte with a timeout of 0 to avoid blocking
        let n = futures_lite::future::or(
            self.stream.peek(&mut buf),
            futures_lite::future::ready(Ok(0)),
        )
        .await
        .map_err(Error::NetworkError)?;

        Ok(n > 0)
    }
}

impl Row {
    /// Get the value of a field by its name.
    pub fn get(&self, name: &str) -> Option<&Data> {
        let index = self.metadata.field_index(name)?;

        self.fields.get(index)
    }

    /// Get the value of a field and parse it to a specific type.
    pub fn get_and_parse<'a, T: FromSql<'a>>(&'a self, name: &str) -> Result<T, BoxError> {
        let data = self
            .get(name)
            .ok_or_else(|| Box::new(FieldNotFound(name.to_owned())))?;

        let field_index = self.metadata.field_index(name).unwrap();
        match self.metadata.fields[field_index].format_code {
            FormatCode::Binary => data.parse_binary(),
            FormatCode::Text => data.parse_text(),
        }
    }
}

impl Display for FieldNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "row doesn't contain field `{}`", &self.0)
    }
}

impl std::error::Error for FieldNotFound {}
