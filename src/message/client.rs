//! Client-to-server messages.

use std::collections::HashMap;

use crate::util::Writer;

/// The type of client-to-server messages.
pub enum MessageType {
    /// The startup message sent by the client.
    Startup,
    /// A message initiating a simple query.
    SimpleQuery,
}

/// The startup message sent by the client.
pub struct Startup {
    /// The user name to connect as.
    user: String,
    /// Other, optional parameters.
    options: HashMap<String, String>,
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

impl From<MessageType> for Option<u8> {
    /// These constants are defined by the protocol.
    fn from(value: MessageType) -> Self {
        let msg_type = match value {
            MessageType::SimpleQuery => b'Q',
            // The startup message has no type byte for historical reasons.
            MessageType::Startup => return None,
        };
        Some(msg_type)
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

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}
