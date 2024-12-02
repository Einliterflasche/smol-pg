use std::net::Ipv4Addr;

use smol_pg::connection::Connection;

#[test]
fn test_simple_query() {
    smol::block_on(actual_main()).unwrap();
}

async fn actual_main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .without_time()
        .with_max_level(tracing::Level::TRACE)
        .init();

    tracing::info!("Creating connection");

    let connection = Connection::create(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), None).await?;
    Ok(())
}
