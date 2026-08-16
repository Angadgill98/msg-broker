use std::error::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;

use crate::consumer::inti::consumer;
use crate::producer::init::producer;





pub struct client{
    socket:OwnedWriteHalf,
    consumer:consumer,
    producer:producer
}


impl client {
    pub async fn init() -> Result<Self, Box<dyn Error>> {
        let socket = client::CreateSocket().await?;

        let (mut reader, writer) = socket.into_split();

        tokio::spawn(async move {
            loop {
                // -------------------------
                // Read ACK
                // -------------------------
                let mut ack_buf = [0u8; 1];

                if let Err(e) = reader.read_exact(&mut ack_buf).await {
                    eprintln!("Server connection closed: {}", e);
                    break;
                }

                let ack = ack_buf[0] == 1;

                // -------------------------
                // Read response length
                // -------------------------
                let mut len_buf = [0u8; 8];

                if let Err(e) = reader.read_exact(&mut len_buf).await {
                    eprintln!("Failed to read response length: {}", e);
                    break;
                }

                let len = u64::from_be_bytes(len_buf) as usize;

                // -------------------------
                // Read response
                // -------------------------
                let mut response = vec![0u8; len];

                if let Err(e) = reader.read_exact(&mut response).await {
                    eprintln!("Failed to read response: {}", e);
                    break;
                }

                // -------------------------
                // Handle response
                // -------------------------

                if ack {
                    println!("Request succeeded");
                    println!("Response: {:?}", response);
                } else {
                    println!(
                        "Request failed: {}",
                        String::from_utf8_lossy(&response)
                    );
                }
            }
        });

        Ok(Self {
            socket: writer,
            consumer: consumer::new(),
            producer: producer::new(),
        })
    }

    pub async fn CreateSocket() -> Result<TcpStream, Box<dyn Error>> {
        let addr = std::env::var("server_addr")
            .map_err(|_| "Environment variable 'server_addr' not defined")?;

        let stream = TcpStream::connect(addr).await?;

        Ok(stream)
    }

    pub async fn insert_topic(
        &mut self,
        topic_name: String,
        partition_no: u64,
    ) -> Result<(), Box<dyn Error>> {
        let op = b"topic_insert";
        let op_len = (op.len() as u64).to_be_bytes();

        let topic_buf = topic_name.as_bytes();
        let topic_len = (topic_buf.len() as u64).to_be_bytes();

        let partition_buf = partition_no.to_be_bytes();

        let mut buf = Vec::new();

        buf.extend_from_slice(&op_len);
        buf.extend_from_slice(op);

        buf.extend_from_slice(&topic_len);
        buf.extend_from_slice(topic_buf);

        buf.extend_from_slice(&partition_buf);

        // Total payload length
        let buf_len = (buf.len() as u64).to_be_bytes();

        let mut final_buf = Vec::with_capacity(8 + buf.len());

        final_buf.extend_from_slice(&buf_len);
        final_buf.extend_from_slice(&buf);

        // Send to server
        self.socket.write_all(&final_buf).await?;

        Ok(())
    }

    pub async fn send_topic_data(
        &mut self,
        topic: String,
        key: Option<String>,
        value: String,
    ) -> Result<(), Box<dyn Error>> {
        let mut buf = Vec::new();

        let op = b"topic_data_insert";
        let op_len = (op.len() as u64).to_be_bytes();

        buf.extend_from_slice(&op_len);
        buf.extend_from_slice(op);

        let topic_buf = topic.as_bytes();
        let topic_len = (topic_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&topic_len);
        buf.extend_from_slice(topic_buf);

        match key {
            Some(s) => {
                let key_buf = s.as_bytes();
                let key_len = (key_buf.len() as u64).to_be_bytes();

                buf.extend_from_slice(&key_len);
                buf.extend_from_slice(key_buf);
            }

            None => {
                buf.extend_from_slice(&0u64.to_be_bytes());
            }
        }

        let value_buf = value.as_bytes();
        let value_len = (value_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&value_len);
        buf.extend_from_slice(value_buf);

        // Total payload length
        let buf_len = (buf.len() as u64).to_be_bytes();

        let mut final_buf = Vec::with_capacity(8 + buf.len());

        final_buf.extend_from_slice(&buf_len);
        final_buf.extend_from_slice(&buf);

        // Send to server
        self.socket.write_all(&final_buf).await?;

        Ok(())
    }

}

