use std::io;

use trakkin_adapter_sdk::{read_launch_request, serve_adapter};
use trakkin_fixture_adapter::FixtureAdapter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let launch = read_launch_request(io::stdin().lock())?;
    let adapter = FixtureAdapter::new(launch.process_instance_id.clone(), Default::default());
    let shutdown = adapter.shutdown_token().cancelled_owned();
    serve_adapter(&launch, adapter, io::stdout(), shutdown).await?;
    Ok(())
}
