//! This module contains everything directly related to the PostgreSQL protocol.

pub mod message;

use message::server::{DataRow, RowDescription};


/// The result of a query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The row description of the query.
    row_description: RowDescription,
    /// The data rows of the query.
    data_rows: Vec<DataRow>,
}

impl QueryResult {
    /// Create a new query result from a row description and data rows.
    pub fn new(row_description: RowDescription, data_rows: Vec<DataRow>) -> Self {
        Self { row_description, data_rows }
    }
}
