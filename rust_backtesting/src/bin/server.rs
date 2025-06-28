use tonic::transport::Server;
use rust_backtesting::{pb::backtest_service_server::BacktestServiceServer, BacktestService};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr_str = std::env::var("BACKTEST_ADDR").unwrap_or_else(|_| "0.0.0.0:9200".to_string());
    let addr = addr_str.parse()?;
    println!("backtest server listening on {}", addr);
    Server::builder()
        .add_service(BacktestServiceServer::new(BacktestService))
        .serve(addr)
        .await?;
    Ok(())
}