//! This module contains the networking part of the connection.
//! Here, we write the messages to and read them from the buffer.

use std::net::IpAddr;

use async_net::TcpStream;
use futures_lite::{AsyncReadExt, AsyncWriteExt};

use crate::{
    message::{client, server},
    util::{self, DecodeError},
    Error,
};

/// A connection to a PostgreSQL server.
///
/// This struct is generic over all transport layers
/// that implement the required traits.
pub struct Connection {
    /// The bi-directional stream that is the transport layer.
    stream: TcpStream,
    /// Here we buffer responses from the server until we handle them.
    response_buffer: Vec<server::Message>,
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
        let mut connection = Self::new(stream);

        // Startup routine
        let startup_message = client::Startup::new("me".to_string(), None, None);
        connection.send_message(&startup_message).await?;

        // Read the response
        let response = connection.read_message().await?;

        // Parse the response
        let message =
            server::Message::try_from(util::Reader::new(&response)).map_err(Error::CodecError)?;

        // Print the message
        println!("{:?}", &message);

        connection.response_buffer.push(message);

        Ok(connection)
    }

    /// Create a new connection from a bi-directional stream.
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            response_buffer: Vec::new(),
        }
    }

    /// Send a message to the server.
    async fn send_message(&mut self, message: impl Into<Vec<u8>>) -> Result<(), Error> {
        self.stream
            .write_all(&message.into())
            .await
            .map_err(Error::NetworkError)?;
        Ok(())
    }
}

impl Connection {
    /// Read a message from the stream, appending it to the buffer (resizing it if necessary).
    async fn read_message(&mut self) -> Result<Vec<u8>, Error> {
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

        Ok(buf)
    }
}
