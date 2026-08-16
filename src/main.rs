mod server;
mod consumer;
mod producer;
mod client;

mod producer_benchmark;

mod kafka_producer_benchmark;
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

    // producer_benchmark::run( &mut client, producer_benchmark::BenchmarkConfig { 
    //     clients: 1, 
    //     topics: 10, 
    //     partitions_per_topic: 2, 
    //     operations_per_partition: 10, 
    //     runs: 1})
    // .await.unwrap();



    
    // let mut clients = Vec::new();

    // for _ in 0..10 {
    //     let mut client = match client::init::client::init().await {
    //     Ok(client) => client,
    //     Err(e) => {
    //         eprintln!("Failed to create client: {}", e);
    //         return;
    //     }
    // };
    //     clients.push(client);
    // }

//     kafka_producer_benchmark::run(
//     &mut clients,
//     kafka_producer_benchmark::BenchmarkConfig {
//         clients: 10,
//         topics: 10,
//         partitions_per_topic: 10,
//         total_records: 10_000,
//         record_size: 1024,
//         warmup_records: 0,
//         runs: 1,
//         throughput: -1,
//     },
// ).await.unwrap();

    println!("Benchmark finished. Press ENTER to exit...");

    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
}