//! This module contains everything directly related to the PostgreSQL protocol.

pub mod message;

use message::server::{DataRow, RowDescription};
