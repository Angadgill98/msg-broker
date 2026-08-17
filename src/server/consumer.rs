use std::net::SocketAddr;



#[derive(Debug)]
pub struct Consumer {
    consumer_addr: SocketAddr,
    start_point: usize,
    offset: usize,
}