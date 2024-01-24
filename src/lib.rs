mod message;

use message::{BackendMessageType, Message, RawMessage};

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};

use async_io::Async;
use futures_lite::prelude::*;

use macro_rules_attribute::apply;
use thiserror_lite::err_enum;

/// The protocol version this client implements:
///
/// `3.0`
const PROTOCOL_VERSION: i32 = 0x00030000;

/// Default postgres server port
const POSTGRES_PORT: u16 = 5432;

#[apply(err_enum)]
#[derive(Debug)]
pub enum Error {
    #[error("couldn't connect to server")]
    ConnectionError(io::Error),
    #[error("failed to send/recieve data")]
    NetworkError(io::Error),
    #[error("recieved message with unknown type")]
    UnknownMessage(u8),
    #[error("couldn't parse expected message")]
    InvalidMessage(BackendMessageType, &'static str),
}

pub struct Client {
    pub stream: Async<TcpStream>,
    options: Options,
}

pub struct Options {
    pub address: SocketAddr,
    pub user_name: String,
    pub database: Option<String>,
}

impl Client {
    pub async fn new(options: Options) -> Result<Client, Error> {
        let stream = Async::<TcpStream>::connect(options.address)
            .await
            .map_err(Error::ConnectionError)?;

        Ok(Client { stream, options })
    }

    pub async fn connect(&mut self) -> Result<(), Error> {
        let startup_message = message::create_startup_message(&self.options);

        self.stream
            .write_all(&startup_message)
            .await
            .map_err(Error::ConnectionError)?;

        Ok(())
    }

    pub async fn send_message(&mut self, message: &RawMessage) -> Result<(), Error> {
        self.send_bytes(&message.buffer).await
    }

    async fn send_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.stream
            .write_all(bytes)
            .await
            .map_err(Error::NetworkError)?;

        self.stream.flush().await.map_err(Error::NetworkError)
    }

    pub async fn recv_message(&mut self) -> Result<Message, Error> {
        // The first byte of each message (execpt the startup message)
        // represents the message type.
        let mut type_ = [0u8];
        self.stream
            .read_exact(&mut type_)
            .await
            .map_err(Error::NetworkError)?;
        let type_ = type_[0];

        // The next 4 bytes of each message represent the length of the
        // message contents including the 4 bytes themselves.
        let mut message_len_buff = [0u8; 4];
        self.stream
            .read_exact(&mut message_len_buff)
            .await
            .map_err(Error::NetworkError)?;

        // subtract 4 to get the number of bytes left to read
        let message_len = u32::from_be_bytes(message_len_buff) - 4;

        // empty buffer of size message_len
        let mut contents = vec![0; message_len as usize];
        // read rest of the message contents
        self.stream
            .read_exact(&mut contents)
            .await
            .map_err(Error::NetworkError)?;

        let mut full_buffer = vec![type_];
        full_buffer.extend_from_slice(&message_len_buff);
        full_buffer.extend_from_slice(&contents);

        Ok(Message {
            type_,
            len: message_len,
            buffer: full_buffer,
        })
    }
}

impl Default for Options {
    fn default() -> Self {
        const DEFAULT_USER: &'static str = "postgres";

        Self {
            address: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, POSTGRES_PORT)),
            user_name: DEFAULT_USER.to_string(),
            database: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::{connect, NoTls};

    use crate::{
        message::{
            backend::{DataRow, ErrorResponse},
            create_query_message, BackendMessageType,
        },
        Client, Options,
    };

    #[tokio::test]
    async fn simple() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = Client::new(Options::default()).await?;

        client.connect().await?;

        loop {
            let message = client.recv_message().await?;
            let message_type = dbg!(BackendMessageType::try_from(message.type_)?);

            match message_type {
                BackendMessageType::ReadyForQuery => {
                    client.send_bytes(&create_query_message("SELECT 5")).await?;
                }
                BackendMessageType::Error => {
                    dbg!(ErrorResponse::try_from(message.buffer.as_slice())?);
                }
                BackendMessageType::DataRow => {
                    let data_row = DataRow::try_from(message.buffer.as_slice())?;
                    dbg!(data_row);
                }
                _ => (),
            }
        }
    }

    #[tokio::test]
    async fn tokio_pg() -> Result<(), Box<dyn std::error::Error>> {
        let (_client, conn) = connect("postgres://postgres:postgres@localhost:5432", NoTls).await?;
        tokio::spawn(async move { conn.await });

        Ok(())
    }
}
