use crate::POSTGRES_DEFAULT_PORT;

#[derive(Debug)]
pub struct Client {
    stream: async_net::TcpStream,
}

impl Client {
    /// Create a new client connected to the given address and port.
    pub async fn new(address: &str, port: Option<u16>) -> Result<Self, crate::Error> {
        let stream = async_net::TcpStream::connect((address, port.unwrap_or(POSTGRES_DEFAULT_PORT))).await?;
        Ok(Self { stream })
    }

    /// Create a new client connected to localhost and the default port.
    pub async fn default() -> Result<Self, crate::Error> {
        Self::new("127.0.0.1", None).await
    }
}

#[cfg(test)]
mod tests {
    use smol::io::{AsyncReadExt, AsyncWriteExt};

    use crate::message::{self, ClientMessage, ServerMessage};

    use super::*;

    #[test]
    fn test_new() -> Result<(), Box<dyn std::error::Error>> {
        smol::block_on(async {
            let mut client = Client::default().await?;

            let startup_message = message::Startup::new("postgres", None);

            client.stream.write_all(&startup_message.encode()).await?;

            loop {
                let mut message_header = [0; 5];
                client.stream.read_exact(&mut message_header).await?;

                let message_type = message_header[0];
                let message_length = u32::from_be_bytes([message_header[1], message_header[2], message_header[3], message_header[4]]);

                let mut bytes = Vec::with_capacity(message_length as usize + 1);
                bytes.copy_from_slice(&message_header);
                client.stream.read_exact(&mut bytes[5..]).await?;

                match message_type {
                    b'R' => {
                        let message = message::Authentication::decode(&bytes)?;
                        println!("{:?}", message);
                    }
                    b'Z' => {
                        let message = message::ReadyForQuery::decode(&bytes)?;
                        println!("{:?}", message);
                    }
                    b'E' => {
                        let message = message::ErrorResponse::decode(&bytes)?;
                        println!("{:?}", message);
                    }
                    _ => {
                        continue;
                    }
                };
            }

            Ok(())
        })
    }
}
