use std::error::Error;

use tokio::net::TcpStream;

use crate::consumer::inti::consumer;
use crate::producer::init::producer;





struct client{
    socket:TcpStream,
    consumer:consumer,
    producer:producer
}


impl client {
    async fn init(&self) -> Result<Self, Box<dyn Error>> {
        let socket = self.CreateSocket().await?;

        Ok(Self {
            socket,
            consumer: consumer::new(),
            producer: producer::new(),
        })
    }

    async fn CreateSocket(&self) -> Result<TcpStream, Box<dyn Error>> {
        let addr = std::env::var("server_addr")
            .map_err(|_| "Environment variable 'server_addr' not defined")?;

        let stream = TcpStream::connect(addr).await?;

        Ok(stream)
    }

    fn insert_topic(&self,topic_name:String,partition_no:u64){
        let op="topic_insert".as_bytes();
        let op_len=(op.len() as u64).to_be_bytes();

        let topic_buf=topic_name.as_bytes();
        let topic_len=(topic_buf.len() as u64).to_be_bytes();

        let partition_buf=partition_no.to_be_bytes();

        let mut buf:Vec<u8>=Vec::new();

        buf.extend_from_slice(&op_len);
        buf.extend_from_slice(op);


        buf.extend_from_slice(&topic_len);
        buf.extend_from_slice(topic_buf);

        buf.extend_from_slice(&partition_buf);

        let buf_len=(buf.len() as u64).to_be_bytes();

        let mut final_buf=Vec::new();

        final_buf.extend_from_slice(&buf_len);
        final_buf.extend_from_slice(&buf);

        self.producer.insert(final_buf);

    }

    fn send_topic_data(&self,topic:String,key:Option<String>,value:String){
        let mut buf:Vec<u8>=Vec::new();

        let op="topic_data_insert".as_bytes();
        let op_len=(op.len() as u64).to_be_bytes();
        
        buf.extend_from_slice(&op_len);
        buf.extend_from_slice(op);

        let topic_buf=topic.as_bytes();
        let topic_len=(topic_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&topic_len);
        buf.extend_from_slice(topic_buf);

        match key {
            Some(s)=>{
                let key_buf=s.as_bytes();
                let key_len=(key_buf.len() as u64).to_be_bytes();
                
                buf.extend_from_slice(&key_len);
                buf.extend_from_slice(key_buf);  
            }
            None=>{
                let key_len=[0u8;8];
                buf.extend_from_slice(&key_len);
            }
        }

        let value_buf=value.as_bytes();
        let value_len=(value_buf.len() as u64).to_be_bytes();

        buf.extend_from_slice(&value_len);
        buf.extend_from_slice(value_buf);


        let buf_len=(buf.len() as u64).to_be_bytes();

        let mut final_buf=Vec::new();

        final_buf.extend_from_slice(&buf_len);
        final_buf.extend_from_slice(&buf);

        self.producer.insert_data(final_buf);

    }
}