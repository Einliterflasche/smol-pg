//! Client-to-server messages.

use std::collections::HashMap;

use crate::util::Writer;

/// The startup message sent by the client.
pub struct Startup {
    /// The user name to connect as.
    user: String,
    /// Other, optional parameters.
    options: HashMap<String, String>,
}


/// A simple query message.
pub struct Query {
    /// The query to send to the server.
    query: String,
}

impl Startup {
    /// Create a new startup message.
    pub fn new(user: String, database: Option<String>, server_options: Option<String>) -> Self {
        let mut options = HashMap::new();

        if let Some(database) = database {
            options.insert("database".into(), database);
        }

        if let Some(server_options) = server_options {
            options.insert("options".into(), server_options);
        }

        Self { user, options }
    }
}

impl From<&Startup> for Vec<u8> {
    fn from(message: &Startup) -> Self {
        let mut writer = Writer::new();

        // Reserve space for the length of the message.
        writer.skip(4);

        // Write the protocol version
        writer.write_i32(crate::PROTOCOL_VERSION);

        // Write the user name
        writer.write_cstring("user");
        writer.write_cstring(&message.user);

        // Write the other options.
        for (key, value) in &message.options {
            writer.write_cstring(key);
            writer.write_cstring(value);
        }

        // Write the null terminator to signal the end of the message.
        writer.write_u8(0);

        // Overwrite the length of the message.
        writer
            .write_i32_at(writer.len() as i32, 0)
            .expect("more than 4 bytes of message content");

        // Finish the message.
        writer.finish()
    }
}

impl Query {
    /// Create a new query message.
    pub fn new(query: String) -> Self {
        Self { query }
    }
}

impl From<&Query> for Vec<u8> {
    fn from(message: &Query) -> Self {
        let mut writer = Writer::new();

        // This is the message type for a simple query.
        writer.write_u8(b'Q');

        // Reserve space for the length field.
        writer.skip(4);

        // Write the query string.
        writer.write_cstring(&message.query);

        // Overwrite the length field (-1 because this excludes the message type).
        writer
            .write_i32_at(writer.len() as i32 - 1, 1)
            .expect("more than 4 bytes of message content");

        // Finish the message.
        writer.finish()
    }
}
