use std::io::{self, IoSlice};
use std::net::TcpStream;

use async_io::Async;
use futures_lite::prelude::*;

thiserror_lite::err_enum! {
    #[derive(Debug)]
    pub enum Error {
        #[error("couldn't connect to server")]
        ConnectionError(io::Error),
        #[error("attempted to connect to invalid address")]
        InvalidAddress(io::Error)
    }
}

pub struct Client {
    pub stream: Async<TcpStream>,
}

impl Client {
    pub async fn new(address: impl Into<std::net::SocketAddr>) -> Result<Client, Error> {
        let stream = Async::<TcpStream>::connect(address)
            .await
            .map_err(|e| Error::ConnectionError(e))?;

        Ok(Client { stream })
    }

    pub async fn write_vectored(&mut self, content: &[u8]) -> io::Result<()> {
        let len = (content.len() as u32 + 4).to_be_bytes();

        let written = self
            .stream
            .write_vectored(&[IoSlice::new(&len), IoSlice::new(content)])
            .await?;

        if written == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "ascac"));
        }

        if written < content.len() + 4 {
            return Err(io::Error::new(io::ErrorKind::Other, "as"));
        }

        Ok(())
    }

    pub async fn write_vectored_with_version(&mut self, content: &[IoSlice<'_>]) -> io::Result<()> {
        const PROT_VERSION: [u8; 4] = 0x00030000u32.to_be_bytes();

        let content_len = content.iter().map(|i| i.len()).sum();

        let content = content
            .into_iter()
            .fold(Vec::with_capacity(content_len), |mut acc, i| {
                acc.extend_from_slice(i);
                acc
            });

        let written = self
            .stream
            .write_vectored(&[
                IoSlice::new(&(content_len as u32 + 8).to_be_bytes()),
                IoSlice::new(&PROT_VERSION),
                IoSlice::new(&content),
            ])
            .await?;

        dbg!(written, content.len() + 8);

        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "couldn't write to stream",
            ));
        }
        if written < content.len() + 8 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "couldn't completely write to stream",
            ));
        }

        Ok(())
    }

    pub async fn read_with_char(&mut self) -> Result<RawResponse, io::Error> {
        let mut head = [0u8; 5];
        self.stream.read_exact(&mut head).await?;

        let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) - 4;

        let mut buf = vec![0; len as usize];
        self.stream.read_exact(&mut buf).await?;

        Ok(RawResponse {
            ty: head[0],
            content: buf,
        })
    }

    async fn start_up(&mut self, user: &str) -> Result<(), io::Error> {
        self.write_vectored_with_version(&[
            IoSlice::new(b"user\0"),
            IoSlice::new(user.as_bytes()),
            IoSlice::new(&[0, 0]),
        ])
        .await?;

        let raw_res = self.read_with_char().await?;
        let res: AuthenticationSasl = (&raw_res).try_into()?;
        dbg!(res.0);

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct RawResponse {
    ty: u8,
    content: Vec<u8>,
}

struct AuthenticationSasl<'a>(Vec<&'a str>);

impl<'a> TryFrom<&'a RawResponse> for AuthenticationSasl<'a> {
    type Error = io::Error;

    fn try_from(raw: &'a RawResponse) -> Result<Self, Self::Error> {
        let auth_type = &raw.content[0..4];
        if auth_type != &[0, 0, 0, 10] {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "invalid auth_type/format",
            ));
        }

        let mechanisms = std::str::from_utf8(&raw.content[4..])
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "invalid utf8"))?;

        Ok(AuthenticationSasl(
            mechanisms.split_terminator('\0').collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::net::ToSocketAddrs;

    use tokio_postgres::{connect, NoTls};

    use crate::Client;

    #[tokio::test]
    async fn simple() -> Result<(), Box<dyn std::error::Error>> {
        let addr = "localhost:5432".to_socket_addrs()?.next().unwrap();
        let mut c = Client::new(addr).await?;

        c.start_up("postgres").await?;

        Ok(())
    }

    #[tokio::test]
    async fn tokio_pg() -> Result<(), Box<dyn std::error::Error>> {
        let (_client, conn) = connect("postgres://postgres:postgres@localhost:5432", NoTls).await?;
        tokio::spawn(async move { conn.await });

        Ok(())
    }
}
