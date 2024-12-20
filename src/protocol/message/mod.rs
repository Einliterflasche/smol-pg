//! Message types for encoding and decoding messages
//! as defined by the PostgreSQL [protocol](https://www.postgresql.org/docs/current/protocol-message-formats.html).

pub mod client;
pub mod server;
pub mod parsing;
