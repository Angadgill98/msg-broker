mod server;
mod consumer;
mod producer;
mod client;


use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let (server_ready_tx, server_ready_rx) =
        oneshot::channel();

    tokio::spawn(async move {
        server::init::Init(server_ready_tx).await;
    });

    // Wait until server has successfully bound
    if server_ready_rx.await.is_err() {
        eprintln!("Server failed to start");
        return;
    }

    // Now server is ready
    let mut client = match client::init::client::init().await {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Failed to create client: {}", e);
            return;
        }
    };

    if let Err(e) =
        client::cli::Cli::init(&mut client).await
    {
        eprintln!("CLI error: {}", e);
    }
}