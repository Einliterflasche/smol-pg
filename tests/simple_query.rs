use std::net::Ipv4Addr;

use smol_pg::{connection::Connection, util::BoxError};

#[test]
fn test_simple_query() {
    smol::block_on(actual_main()).unwrap();
}

async fn actual_main() -> Result<(), BoxError> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .without_time()
        .with_max_level(tracing::Level::TRACE)
        .init();

    tracing::info!("Creating connection");

    let mut connection =
        Connection::create(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), None).await?;

    tracing::info!("Connection created");

    let rows = connection.query("SELECT 1").await?;

    let rows: Vec<i32> = rows
        .iter()
        .map(|row| row.get_and_parse::<i32>("?column?"))
        .collect::<Result<Vec<i32>, BoxError>>()?;

    tracing::debug!(rows=?rows, "rows");

    Ok(())
}
