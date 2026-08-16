use std::{error::Error, hash::DefaultHasher, io::Write, net::SocketAddr};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::{mpsc::{self, Sender}, oneshot}};
use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use std::hash::{Hash, Hasher};



struct server{
    shard_map:HashMap<Shard,RwLock<TopicMap>>,
    clients:HashMap<SocketAddr,RwLock<TcpStream> >,
    shard_count:usize,
    request_handler:Sender<(Arc<server>,Vec<u8>,Vec<u8>,SocketAddr)>,
    partition_worker_pool: Sender<PartitionWorkerRequest>,
    response_writer_singal:Sender<ResponseRequest>,
}



struct TopicMap{
    map:HashMap<Vec<u8>,topic>
}
impl TopicMap {
    fn new()->TopicMap{
        Self{
            map:HashMap::new()
        }
    }
    fn insert(&mut self,topic_buf: Vec<u8>, topic: topic){
        self.map.insert(topic_buf, topic);
    }
    fn get(& self,topic_buf: &Vec<u8>)-> Option<&topic>{
        self.map.get(topic_buf)

    }
}

// #[derive(Hash, Eq, PartialEq)]
// struct topic_name{}


struct topic{
    partition_no:usize,
    partitions: HashMap<usize, Arc<RwLock<Partition>>>,
}
impl topic {
    fn new(topic_name: &Vec<u8>, partition_no: usize) -> Self {
        let partitions = CreatePartitions(topic_name, partition_no);

        Self {
            partition_no,
            partitions,
        }
    }
}
fn CreatePartitions(topic_name: &[u8], partition_no: usize)-> HashMap<usize, Arc<RwLock<Partition>>> {
    let topic_name = String::from_utf8(topic_name.to_vec()).unwrap();

    let mut partitions = HashMap::new();

    for i in 0..partition_no {
        let file_name = format!("{}_partition_{}.log", topic_name, i);

        std::fs::File::create(&file_name).unwrap();

        let partition = Partition {
            id: i,
            file_name,
            consumers: Vec::new(),
        };

        partitions.insert(
            i,
            Arc::new(RwLock::new(partition)),
        );
    }

    partitions
}
type WorkerRequest = (
    Arc<server>,
    Vec<u8>,
    Vec<u8>,
    Option<oneshot::Receiver<bool>>,
    oneshot::Sender<bool>,
    SocketAddr
);
type PartitionWorkerRequest = (
    Arc<server>,
    Arc<RwLock<Partition>>,
    Vec<u8>,
    SocketAddr
);
type ResponseRequest = (
    Arc<server>,
    SocketAddr,
    bool,
    Vec<u8>,
);

struct Partition {
    id: usize,
    file_name: String,
    consumers:Vec<Consumer>
}
impl Partition {
    fn WriteTOFile(&self, value: Vec<u8>) -> Result<(), Box<dyn std::error::Error +Send+Sync>> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_name)?;

        file.write_all(&value)?;
        file.write_all(b"\n")?;

        Ok(())
    }
}

struct Consumer{
    consumer_addr:SocketAddr,
    start_point:usize,
    offset:usize,
}

#[derive(Hash, Eq, PartialEq)]
struct Shard(usize);

impl server{
    fn new(shard_count:usize)->Self{
        let requesthandler_sender=server::RequestHandler();
        let partition_worker_pool =server::PartitionWorkerPool(4);
        let respnse_signla=ResponseWriter();
        Self{
            shard_map:CreateShardMap(shard_count.clone()),
            clients:HashMap::new(),
            shard_count:shard_count,
            request_handler:requesthandler_sender,
            partition_worker_pool:partition_worker_pool,
            response_writer_singal:respnse_signla
        }
    }

    // fn RequestHandler()->Sender<(Arc<server>,Vec<u8>,Vec<u8>)>{
    //     let(mut sender,mut reciver)=mpsc::channel::<(Arc<server>,Vec<u8>, Vec<u8>)>(1024);

    //     tokio::spawn(async move{
    //         let mut  reciver_for_prev_Sender:Arc<oneshot::Receiver<bool>>;
    //         while let Some((server,operation,payload)) = reciver.recv().await {
    //             let(sender,reciever)=oneshot::channel::<bool>();
    //             reciver_for_prev_Sender=Arc::new(reciever);
                
    //             tokio::spawn(async move{
    //                 //this will send singal and the prev
    //             });
            
                
                
    //         }
    //     });

    //     sender
    // }

    fn RequestHandler() -> mpsc::Sender<(Arc<server>,Vec<u8>,Vec<u8>,SocketAddr)> {
        let (sender, mut receiver) =
            mpsc::channel::<(Arc<server>, Vec<u8>, Vec<u8>,SocketAddr)>(1024);

        let worker_queue = server::WorkerPool(4);

        tokio::spawn(async move {
            let mut previous_receiver: Option<oneshot::Receiver<bool>> = None;

            while let Some((server, operation, payload,client_addr)) = receiver.recv().await {

                // Signal for THIS request
                let (signal_sender, signal_receiver) =
                    oneshot::channel::<bool>();

                // Receiver that THIS request must wait for
                let previous = previous_receiver.take();

                // This request's receiver becomes the
                // dependency for the NEXT request
                previous_receiver = Some(signal_receiver);

                // Send request to worker pool
                worker_queue
                    .send((
                        server,
                        operation,
                        payload,
                        previous,
                        signal_sender,
                        client_addr
                    ))
                    .await
                    .unwrap();
            }
        });

        sender
    }

    fn WorkerPool(worker_count: usize) -> mpsc::Sender<WorkerRequest> {
        let (sender, mut receiver) =
            mpsc::channel::<WorkerRequest>(1024);

        let mut worker_senders = Vec::new();
        for _ in 0..worker_count {
            let (worker_sender, mut worker_receiver) =
                mpsc::channel::<WorkerRequest>(256);

            worker_senders.push(worker_sender);

            tokio::spawn(async move {
                while let Some((
                    server,
                    operation,
                    payload,
                    previous_receiver,
                    signal_sender,
                    cliet_addr
                )) = worker_receiver.recv().await {

                    // processing can happen concurrently
                    let result = server.HandleOperation(operation, payload).await;
                    
                    match result {
                        Ok(Some((value, partition))) => {

                            if let Some(previous_receiver) = previous_receiver {
                                let _ = previous_receiver.await;
                            }

                            // Write/send value using this partition
                            server.partition_worker_pool
                                .send((Arc::clone(&server),partition, value,cliet_addr))
                                .await
                                .unwrap();

                            let _ = signal_sender.send(true);
                        }

                        Ok(None) => {
                            // Operation succeeded but has no result.
                            let _ = signal_sender.send(true);
                        }

                        Err(e) => {
                            eprintln!("HandleOperation failed: {}", e);

                            let _ = signal_sender.send(true);
                        }
                    }

                   
                }
            });
        }

        // Dispatcher
        tokio::spawn(async move {
            let mut next_worker = 0;

            while let Some(request) = receiver.recv().await {
                worker_senders[next_worker]
                    .send(request)
                    .await
                    .unwrap();

                next_worker = (next_worker + 1) % worker_senders.len();
            }
        });

        sender
    }
    
    fn PartitionWorkerPool(worker_count: usize) -> mpsc::Sender<PartitionWorkerRequest> {
        let (sender, mut receiver) =
            mpsc::channel::<PartitionWorkerRequest>(1024);

        let mut worker_senders = Vec::new();

        for _ in 0..worker_count {
            let (worker_sender, mut worker_receiver) =
                mpsc::channel::<PartitionWorkerRequest>(256);

            worker_senders.push(worker_sender);

            tokio::spawn(async move {
                while let Some((server,partition, value,client_addr)) =
                    worker_receiver.recv().await
                {
                    
                    let partition_guard = partition.write().await;
                    match partition_guard.WriteTOFile(value) {
                        Ok(()) => {
                            // File write succeeded
                            if let Err(e) = server
                                .response_writer_singal
                                .send((
                                    Arc::clone(&server),
                                    client_addr,
                                    true,
                                    Vec::new(),
                                ))
                                .await
                            {
                                eprintln!("Failed to queue success response: {}", e);
                            }
                        }

                        Err(e) => {
                            eprintln!("Failed to write partition: {}", e);

                            // File write failed
                            let error_message = e.to_string().into_bytes();

                            if let Err(send_err) = server
                                .response_writer_singal
                                .send((
                                    Arc::clone(&server),
                                    client_addr,
                                    false,
                                    error_message,
                                ))
                                .await
                            {
                                eprintln!("Failed to queue error response: {}", send_err);
                            }
                        }
                    }

                    println!(
                        "Worker writing to partition {} adn file_name {}",
                        partition_guard.id,partition_guard.file_name
                    );
                }
            });
        }

        // Dispatcher
        tokio::spawn(async move {
            let mut next_worker = 0;

            while let Some(request) = receiver.recv().await {
                worker_senders[next_worker]
                    .send(request)
                    .await
                    .unwrap();

                next_worker =
                    (next_worker + 1) % worker_senders.len();
            }
        });

        sender
    }

    async fn HandleOperation(&self,operation:Vec<u8>,payload: Vec<u8>)->Result<Option<(Vec<u8>, Arc<RwLock<Partition>>)>,Box<dyn std::error::Error+Send+Sync>>{
        let operation=String::from_utf8(operation).unwrap();
    
        match operation.trim() {
            "topic_insert"=>{
                let (topic_name_buf,payload)=Simplify(payload);

                let (partition_no,_)=Simplify(payload);

                let shard=self.GetShard(&topic_name_buf, self.shard_count);

                let topic_map =self.shard_map.get(&shard).unwrap();

                let mut topic_map_gaurd=topic_map.write().await;

                let partition_no = u64::from_be_bytes(
                    partition_no.try_into().unwrap()
                ) as usize;

                let topic=topic::new(&topic_name_buf, partition_no);

                topic_map_gaurd.insert(topic_name_buf, topic);

                drop(topic_map_gaurd);

                return Ok(None)

            }
            "topic_data_insert"=>{
                let (topic_name_buf,payload)=Simplify(payload);

                let (key_buf,payload)=Simplify(payload);

                let (value_buf,payload)=Simplify(payload);


                let shard=self.GetShard(&topic_name_buf, self.shard_count);

                let topic_map=self.shard_map.get(&shard).unwrap();

                let topic_map_guard=topic_map.read().await;

                let topic =topic_map_guard.get(&topic_name_buf).unwrap();

                let partition_no=topic.partition_no;

                let key_buf_hash=self.GetHash(&key_buf) as usize;

                let partition_id = key_buf_hash % partition_no;

                let partition= topic.partitions.get(&partition_id).unwrap();

                let partition=Arc::clone(partition);
                drop(topic_map_guard);

                
                return Ok(Some((value_buf,partition)))


                //writeto partiton log file 
            }
            _=>{

            }

        }
            Ok(None)
    }

    fn GetShard(&self,topic: &[u8], shard_count: usize) -> Shard {
        let mut hasher = DefaultHasher::new();

        topic.hash(&mut hasher);

        let hash = hasher.finish();

        Shard((hash as usize) % shard_count)
    }

    fn GetHash(&self,data: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();

        data.hash(&mut hasher);

        hasher.finish()
    }
    
}

fn CreateShardMap(shard_count:usize)->HashMap<Shard, RwLock<TopicMap> >{
    let mut shards = HashMap::new();
    for i in 0..shard_count{
        shards.insert(
            Shard(i),
            RwLock::new(TopicMap {
                map: HashMap::new(),
            }),
        );
    }
    shards
}

fn ResponseWriter() -> mpsc::Sender<ResponseRequest> {
    let (sender, mut receiver) =
        mpsc::channel::<ResponseRequest>(1024);

    tokio::spawn(async move {
        while let Some((server, client_addr, ack,response)) =
            receiver.recv().await
        {
            let Some(client) = server.clients.get(&client_addr) else {
                eprintln!("Client not found: {}", client_addr);
                continue;
            };

            let mut client = client.write().await;
            let response_len = response.len() as u64;

            let mut output = Vec::with_capacity(1 + 8 + response.len());

            // ACK
            output.push(if ack { 1u8 } else { 0u8 });

            // Response length
            output.extend_from_slice(&response_len.to_be_bytes());

            // Actual response
            output.extend_from_slice(&response);

            if let Err(e) = client.write_all(&output).await {
                eprintln!(
                    "Failed to send response to {}: {}",
                    client_addr, e
                );
            }
        }
    });

    sender
}
pub async fn Init(server_ready: tokio::sync::oneshot::Sender<()>){
    let socket=CreateSocket().await.unwrap();
    let shard_count:usize=10;    
    let server=Arc::new(server::new(shard_count));
    let _ = server_ready.send(());
    loop {
        let (mut stream,client_addr)=socket.accept().await.unwrap();

        let server_client=Arc::clone(&server);
        
        let mut buf_len=[0u8;8];

        stream.read_exact(&mut buf_len).await.unwrap();

        let len = u64::from_be_bytes(buf_len);

        let mut buf = vec![0u8; len as usize];

        stream.read_exact(&mut buf).await.unwrap();



        let (opeation,payload)=Simplify(buf);
        server_client.request_handler.send((Arc::clone(&server),opeation,payload,client_addr)).await.unwrap();

        
    }


}

fn Simplify(buf: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
    let len = u64::from_be_bytes(buf[..8].try_into().unwrap());

    let buf = buf[8..8 + len as usize].to_vec();
    let remaining = buf[8 + len as usize..].to_vec();

    ( buf, remaining)
}




async fn CreateSocket() -> Result<TcpListener, Box<dyn Error>> {
    let addr = std::env::var("server_addr")
        .map_err(|_| "Environment variable 'server_addr' not defined")?;

    let socket = TcpListener::bind(&addr).await?;

    Ok(socket)
}