// use std::{error::Error, hash::DefaultHasher, io::Write, net::SocketAddr};

// use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream, tcp::OwnedWriteHalf}, sync::{mpsc::{self, Sender}, oneshot}};
// use std::{collections::HashMap, sync::Arc};

// use tokio::sync::RwLock;

// use std::hash::{Hash, Hasher};



// struct server{
//     shard_map:HashMap<Shard,Arc<RwLock<TopicMap>>>,
//     clients:HashMap<SocketAddr,Arc<RwLock<OwnedWriteHalf>> >,
//     shard_count:usize,
//     request_handler:Sender<(Arc<RwLock<server>>,Vec<u8>,Vec<u8>,SocketAddr)>,
//     partition_worker_pool: Sender<PartitionWorkerRequest>,
//     response_writer_singal:Sender<ResponseRequest>,
// }



// struct TopicMap{
//     map:HashMap<Vec<u8>,topic>
// }
// impl TopicMap {
//     fn new()->TopicMap{
//         Self{
//             map:HashMap::new()
//         }
//     }
//     fn insert(&mut self,topic_buf: Vec<u8>, topic: topic){
//         self.map.insert(topic_buf, topic);
//     }
//     fn get(& self,topic_buf: &Vec<u8>)-> Option<&topic>{
//         self.map.get(topic_buf)

//     }
// }

// // #[derive(Hash, Eq, PartialEq)]
// // struct topic_name{}


// struct topic{
//     partition_no:usize,
//     partitions: HashMap<usize, Arc<RwLock<Partition>>>,
// }
// impl topic {
//     fn new(topic_name: &Vec<u8>, partition_no: usize) -> Self {
//         let partitions = CreatePartitions(topic_name, partition_no);

//         Self {
//             partition_no,
//             partitions,
//         }
//     }
// }
// fn CreatePartitions(topic_name: &[u8], partition_no: usize)-> HashMap<usize, Arc<RwLock<Partition>>> {
//     let topic_name = String::from_utf8(topic_name.to_vec()).unwrap();

//     let mut partitions = HashMap::new();

//     for i in 0..partition_no {
//         let file_name = format!("{}_partition_{}.log", topic_name, i);

//         std::fs::File::create(&file_name).unwrap();

//         let partition = Partition {
//             id: i,
//             file_name,
//             consumers: Vec::new(),
//         };

//         partitions.insert(
//             i,
//             Arc::new(RwLock::new(partition)),
//         );
//     }

//     partitions
// }
// type WorkerRequest = (
//     Arc<RwLock<server>>,
//     Vec<u8>,
//     Vec<u8>,
//     Option<oneshot::Receiver<bool>>,
//     oneshot::Sender<bool>,
//     SocketAddr
// );
// type PartitionWorkerRequest = (
//     Arc<RwLock<server>>,
//     Arc<RwLock<Partition>>,
//     Vec<u8>,
//     SocketAddr
// );
// type ResponseRequest = (
//     Arc<RwLock<server>>,

//     SocketAddr,
//     bool,
//     Vec<u8>,
// );
// type ConsumerWorkerRequest = (
//     Arc<server>,
//     Arc<RwLock<Partition>>,
//     SocketAddr,
// );

// struct Partition {
//     id: usize,
//     file_name: String,
//     consumers:Vec<Consumer>
// }
// impl Partition {
//     fn WriteTOFile(&self, value: Vec<u8>) -> Result<(), Box<dyn std::error::Error +Send+Sync>> {
//         let mut file = std::fs::OpenOptions::new()
//             .create(true)
//             .append(true)
//             .open(&self.file_name)?;

//         file.write_all(&value)?;
//         file.write_all(b"\n")?;

//         Ok(())
//     }
// }

// struct Consumer{
//     consumer_addr:SocketAddr,
//     start_point:usize,
//     offset:usize,
// }

// #[derive(Hash, Eq, PartialEq)]
// struct Shard(usize);

// impl server{
//     fn new(shard_count:usize)->Self{
//         let requesthandler_sender=server::RequestHandler();
//         let partition_worker_pool =server::PartitionWorkerPool(4);
//         let respnse_signla=ResponseWriter();
//         Self{
//             shard_map:CreateShardMap(shard_count.clone()),
//             clients:HashMap::new(),
//             shard_count:shard_count,
//             request_handler:requesthandler_sender,
//             partition_worker_pool:partition_worker_pool,
//             response_writer_singal:respnse_signla
//         }
//     }

//     // fn RequestHandler()->Sender<(Arc<server>,Vec<u8>,Vec<u8>)>{
//     //     let(mut sender,mut reciver)=mpsc::channel::<(Arc<server>,Vec<u8>, Vec<u8>)>(1024);

//     //     tokio::spawn(async move{
//     //         let mut  reciver_for_prev_Sender:Arc<oneshot::Receiver<bool>>;
//     //         while let Some((server,operation,payload)) = reciver.recv().await {
//     //             let(sender,reciever)=oneshot::channel::<bool>();
//     //             reciver_for_prev_Sender=Arc::new(reciever);
                
//     //             tokio::spawn(async move{
//     //                 //this will send singal and the prev
//     //             });
            
                
                
//     //         }
//     //     });

//     //     sender
//     // }

//     fn RequestHandler() -> mpsc::Sender<(Arc<RwLock<server>>,Vec<u8>,Vec<u8>,SocketAddr)> {
//         let (sender, mut receiver) =
//             mpsc::channel::<(Arc<RwLock<server>>, Vec<u8>, Vec<u8>,SocketAddr)>(1024);

//         let worker_queue = server::WorkerPool(4);

//         tokio::spawn(async move {
//             let mut previous_receiver: Option<oneshot::Receiver<bool>> = None;

//             while let Some((server, operation, payload,client_addr)) = receiver.recv().await {

//                 // Signal for THIS request
//                 let (signal_sender, signal_receiver) =
//                     oneshot::channel::<bool>();

//                 // Receiver that THIS request must wait for
//                 let previous = previous_receiver.take();

//                 // This request's receiver becomes the
//                 // dependency for the NEXT request
//                 previous_receiver = Some(signal_receiver);

//                 // Send request to worker pool
//                 worker_queue
//                     .send((
//                         server,
//                         operation,
//                         payload,
//                         previous,
//                         signal_sender,
//                         client_addr
//                     ))
//                     .await
//                     .unwrap();
//             }
//         });

//         sender
//     }

//     fn WorkerPool(worker_count: usize) -> mpsc::Sender<WorkerRequest> {
//         let (sender, mut receiver) =
//             mpsc::channel::<WorkerRequest>(1024);

//         let mut worker_senders = Vec::new();
//         for _ in 0..worker_count {
//             let (worker_sender, mut worker_receiver) =
//                 mpsc::channel::<WorkerRequest>(256);

//             worker_senders.push(worker_sender);

//             tokio::spawn(async move {
//                 while let Some((
//                     server,
//                     operation,
//                     payload,
//                     previous_receiver,
//                     signal_sender,
//                     cliet_addr
//                 )) = worker_receiver.recv().await {

//                     // processing can happen concurrently
//                     let result = server::HandleOperation(&server,operation,payload).await;
                    
//                     match result {
//                         Ok(Some((value, partition))) => {

//                             if let Some(previous_receiver) = previous_receiver {
//                                 let _ = previous_receiver.await;
//                             }
        

//                             // // Write/send value using this partition
//                             // server.read().await.partition_worker_pool
//                             //     .send((Arc::clone(&server),partition, value,cliet_addr))
//                             //     .await
//                             //     .unwrap();

//                             let partition_worker_pool = {
//                             let server_guard = server.read().await;
//                                 server_guard.partition_worker_pool.clone()
//                             };

//                             partition_worker_pool
//                                 .send((
//                                     Arc::clone(&server),
//                                     partition,
//                                     value,
//                                     cliet_addr,
//                                 ))
//                                 .await
//                                 .unwrap();
//                             let _ = signal_sender.send(true);
//                         }

//                         Ok(None) => {
//                             // Operation succeeded but has no result.
//                             let _ = signal_sender.send(true);
//                         }

//                         Err(e) => {
//                             eprintln!("HandleOperation failed: {}", e);

//                             let _ = signal_sender.send(true);
//                         }
//                     }

                   
//                 }
//             });
//         }

//         // Dispatcher
//         tokio::spawn(async move {
//             let mut next_worker = 0;

//             while let Some(request) = receiver.recv().await {
//                 worker_senders[next_worker]
//                     .send(request)
//                     .await
//                     .unwrap();

//                 next_worker = (next_worker + 1) % worker_senders.len();
//             }
//         });

//         sender
//     }
    
//     fn PartitionWorkerPool(worker_count: usize) -> mpsc::Sender<PartitionWorkerRequest> {
//         let (sender, mut receiver) =
//             mpsc::channel::<PartitionWorkerRequest>(1024);

//         let mut worker_senders = Vec::new();

//         for _ in 0..worker_count {
//             let (worker_sender, mut worker_receiver) =
//                 mpsc::channel::<PartitionWorkerRequest>(256);

//             worker_senders.push(worker_sender);

//             tokio::spawn(async move {
//                 while let Some((server,partition, value,client_addr)) =
//                     worker_receiver.recv().await
//                 {
                    
//                     let partition_guard = partition.write().await;
//                     let response_writer_signal = {
//                         let server_guard = server.read().await;
//                         server_guard.response_writer_singal.clone()
//                     };

//                     match partition_guard.WriteTOFile(value) {
//                         Ok(()) => {
//                             if let Err(e) = response_writer_signal
//                                 .send((
//                                     Arc::clone(&server),
//                                     client_addr,
//                                     true,
//                                     Vec::new(),
//                                 ))
//                                 .await
//                             {
//                                 eprintln!("Failed to queue success response: {}", e);
//                             }
//                         }

//                         Err(e) => {
//                             let error_message = e.to_string().into_bytes();

//                             if let Err(send_err) = response_writer_signal
//                                 .send((
//                                     Arc::clone(&server),
//                                     client_addr,
//                                     false,
//                                     error_message,
//                                 ))
//                                 .await
//                             {
//                                 eprintln!("Failed to queue error response: {}", send_err);
//                             }
//                         }
//                     }
//                     println!(
//                         "Worker writing to partition {} adn file_name {}",
//                         partition_guard.id,partition_guard.file_name
//                     );
//                 }
//             });
//         }

//         // Dispatcher
//         tokio::spawn(async move {
//             let mut next_worker = 0;

//             while let Some(request) = receiver.recv().await {
//                 worker_senders[next_worker]
//                     .send(request)
//                     .await
//                     .unwrap();

//                 next_worker =
//                     (next_worker + 1) % worker_senders.len();
//             }
//         });

//         sender
//     }

//     async fn HandleOperation(server:&Arc<RwLock<server>>,operation:Vec<u8>,payload: Vec<u8>)->Result<Option<(Vec<u8>, Arc<RwLock<Partition>>)>,Box<dyn std::error::Error+Send+Sync>>{
//         let operation=String::from_utf8(operation).unwrap();
        
//         match operation.trim() {
//             "topic_insert" => {
//             let (topic_name_buf, payload) = Simplify(payload);

//             let partition_no =
//                 u64::from_be_bytes(
//                     payload.try_into().unwrap()
//                 ) as usize;

//             let topic_map = {
//                 let server_guard = server.read().await;

//                 let shard = server_guard.GetShard(
//                     &topic_name_buf,
//                     server_guard.shard_count,
//                 );

//                 let topic_map =
//                     server_guard.shard_map.get(&shard).unwrap();

//                 let topic_map=Arc::clone(topic_map);
//                 topic_map
//             };


//             let mut topic_map_guard =
//                 topic_map.write().await;

//             let topic =
//                 topic::new(
//                     &topic_name_buf,
//                     partition_no,
//                 );

//             topic_map_guard.insert(
//                 topic_name_buf,
//                 topic,
//             );


//             return Ok(None)
//         }
//             "topic_data_insert" => {
//                 let (topic_name_buf, payload) =
//                     Simplify(payload);

//                 let (key_buf, payload) =
//                     Simplify(payload);

//                 let (value_buf, _) =
//                     Simplify(payload);

//                 let partition = {
//                     let server_guard = server.read().await;

//                     let shard =
//                         server_guard.GetShard(
//                             &topic_name_buf,
//                             server_guard.shard_count,
//                         );

//                     let topic_map =
//                         server_guard
//                             .shard_map
//                             .get(&shard)
//                             .unwrap();

//                     let topic_map_guard =
//                         topic_map.read().await;

//                     let topic =
//                         topic_map_guard
//                             .get(&topic_name_buf)
//                             .unwrap();

//                     let partition_no =
//                         topic.partition_no;

//                     let key_buf_hash =
//                         server_guard
//                             .GetHash(&key_buf) as usize;

//                     let partition_id =
//                         key_buf_hash % partition_no;

//                     let partition =
//                         topic.partitions
//                             .get(&partition_id)
//                             .unwrap();

//                     Arc::clone(partition)
//                 };

//                 // server_guard and topic_map_guard
//                 // are both dropped here.

//                 return Ok(Some((
//                     value_buf,
//                     partition,
//                 )))
//             }
//             _=>{

//             }

//         }
//             Ok(None)
//     }

//     fn GetShard(&self,topic: &[u8], shard_count: usize) -> Shard {
//         let mut hasher = DefaultHasher::new();

//         topic.hash(&mut hasher);

//         let hash = hasher.finish();

//         Shard((hash as usize) % shard_count)
//     }

//     fn GetHash(&self,data: &[u8]) -> u64 {
//         let mut hasher = DefaultHasher::new();

//         data.hash(&mut hasher);

//         hasher.finish()
//     }
    
// }

// fn CreateShardMap(shard_count:usize)->HashMap<Shard, Arc<RwLock<TopicMap>> >{
//     let mut shards = HashMap::new();
//     for i in 0..shard_count{
//         shards.insert(
//             Shard(i),
//             Arc::new(RwLock::new(TopicMap {
//                 map: HashMap::new(),
//             })),
//         );
//     }
//     shards
// }

// fn ResponseWriter() -> mpsc::Sender<ResponseRequest> {
//     let (sender, mut receiver) =
//         mpsc::channel::<ResponseRequest>(1024);

//     tokio::spawn(async move {
//         while let Some((server, client_addr, ack, response)) =
//             receiver.recv().await
//         {
//             // Get the client while holding the server read lock
//             let client = {
//                 let server_guard = server.read().await;

//                 let Some(client) =
//                     server_guard.clients.get(&client_addr)
//                 else {
//                     eprintln!("Client not found: {}", client_addr);
//                     continue;
//                 };

//                 Arc::clone(client)
//             };

//             // server read guard is dropped here

//             let mut client_guard = client.write().await;

//             let response_len = response.len() as u64;

//             let mut output =
//                 Vec::with_capacity(1 + 8 + response.len());

//             // ACK
//             output.push(if ack { 1 } else { 0 });

//             // Response length
//             output.extend_from_slice(
//                 &response_len.to_be_bytes()
//             );

//             // Response
//             output.extend_from_slice(&response);

//             if let Err(e) =
//                 client_guard.write_all(&output).await
//             {
//                 eprintln!(
//                     "Failed to send response to {}: {}",
//                     client_addr, e
//                 );
//             }
//         }
//     });

//     sender
// }





// pub async fn Init(server_ready: tokio::sync::oneshot::Sender<()>){
//     let socket=CreateSocket().await.unwrap();
//     let shard_count:usize=10;    
//     let server=Arc::new(RwLock::new(server::new(shard_count)));
//     let _ = server_ready.send(());
//     println!("Server started");

//     loop {
//         let (stream, client_addr) =
//             socket.accept().await.unwrap();
//         let server = Arc::clone(&server);
//         tokio::spawn(async move {
//             let (mut reader, writer) = stream.into_split();

//             let mut server_guard = server.write().await;

//             let writer_stream =Arc::new(RwLock::new(writer));
//             server_guard.clients.insert(client_addr, writer_stream);

//             drop(server_guard);

//             let server=Arc::clone(&server);
//             // let mut clients = server_guard.clients.write().await;

//             // clients.insert(
//             //     client_addr,
//             //     RwLock::new(stream),
//             // );

//             loop {
//                 // --------------------------------
//                 // Read number of requests in batch
//                 // --------------------------------
//                 let mut count_buf = [0u8; 8];

//                 if let Err(e) = reader.read_exact(&mut count_buf).await {
//                     eprintln!(
//                         "Client {} disconnected: {}",
//                         client_addr, e
//                     );
//                     break;
//                 }

//                 let request_count =
//                     u64::from_be_bytes(count_buf) as usize;

//                 // Read request length
//                 let mut allreq_buf_len = [0u8; 8];

//                 if let Err(e) =
//                     reader.read_exact(&mut allreq_buf_len).await
//                 {
//                     eprintln!(
//                         "Failed reading request length from {}: {}",
//                         client_addr, e
//                     );
//                     break;
//                 }

//                 let len =
//                     u64::from_be_bytes(allreq_buf_len) as usize;

//                 let mut all_req_buf = vec![0u8; len];

//                 if let Err(e) =
//                     reader.read_exact(&mut all_req_buf).await
//                 {
//                     eprintln!(
//                         "Failed reading request from {}: {}",
//                         client_addr, e
//                     );
//                     break;
//                 }

//                 // --------------------------------
//                 // Read every request in the batch
//                 // --------------------------------
//                 let mut remaining_buf = all_req_buf;

//                 for _ in 0..request_count {
//                     // Read one request from the remaining buffer
//                     let (request, remaining) =
//                         Simplify(remaining_buf);

//                     // The current request has now been consumed.
//                     // Continue with whatever remains.
//                     remaining_buf = remaining;
//                     // Parse operation + payload from this request
//                     let (operation, payload) =
//                         Simplify(request);

//                     let request_handler = {
//                         let server_guard = server.read().await;
//                         server_guard.request_handler.clone()
//                     };
//                     if let Err(e) = request_handler
//                         .send((
//                             Arc::clone(&server),
//                             operation,
//                             payload,
//                             client_addr,
//                         ))
//                         .await
//                     {
//                         eprintln!(
//                             "Failed to queue request from {}: {}",
//                             client_addr,
//                             e
//                         );

//                         break;
//                     }
//                 }
//             }
//         });
//     }


// }

// fn Simplify(buf: Vec<u8>) -> (Vec<u8>, Vec<u8>) {
//     // println!("{:?}",buf);
//     let len = u64::from_be_bytes(
//         buf[..8].try_into().unwrap()
//     ) as usize;

//     let value = buf[8..8 + len].to_vec();

//     let remaining = buf[8 + len..].to_vec();

//     (value, remaining)
// }




// async fn CreateSocket() -> Result<TcpListener, Box<dyn Error>> {
//     let addr = std::env::var("server_addr")
//         .map_err(|_| "Environment variable 'server_addr' not defined")?;

//     let socket = TcpListener::bind(&addr).await?;

//     Ok(socket)
// }




use std::{
    collections::HashMap,
    error::Error,
    hash::DefaultHasher,
    io::Write,
    net::SocketAddr,
    sync::Arc,
};

use std::hash::{Hash, Hasher};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpListener},
    sync::{
        mpsc::{self, Sender},
        oneshot,
        RwLock,
    },
};
#[derive(Debug)]

struct server {
    shard_map: HashMap<Shard, Arc<RwLock<TopicMap>>>,
    clients: HashMap<SocketAddr, Arc<RwLock<OwnedWriteHalf>>>,
    shard_count: usize,
    request_handler:
        Sender<(Arc<RwLock<server>>, Vec<u8>, Vec<u8>, SocketAddr)>,
    partition_worker_pool: Sender<PartitionWorkerRequest>,
    response_writer_singal: Sender<ResponseRequest>,
}
#[derive(Debug)]

struct TopicMap {
    map: HashMap<Vec<u8>, topic>,
}

impl TopicMap {
    fn new() -> TopicMap {
        Self {
            map: HashMap::new(),
        }
    }

    fn insert(
        &mut self,
        topic_buf: Vec<u8>,
        topic: topic,
    ) {
        self.map.insert(topic_buf, topic);
    }

    fn get(
        &self,
        topic_buf: &Vec<u8>,
    ) -> Option<&topic> {
        self.map.get(topic_buf)
    }
}
#[derive(Debug)]

struct topic {
    partition_no: usize,
    partitions: HashMap<usize, Arc<RwLock<Partition>>>,
}

impl topic {
    fn new(
        topic_name: &Vec<u8>,
        partition_no: usize,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let partitions =
            CreatePartitions(topic_name, partition_no)?;

        Ok(Self {
            partition_no,
            partitions,
        })
    }
}

fn CreatePartitions(
    topic_name: &[u8],
    partition_no: usize,
) -> Result<
    HashMap<usize, Arc<RwLock<Partition>>>,
    Box<dyn Error + Send + Sync>,
> {
    let topic_name =
        String::from_utf8(topic_name.to_vec())
            .map_err(|e| {
                format!(
                    "Invalid UTF-8 topic name: {}",
                    e
                )
            })?;

    if partition_no == 0 {
        return Err(
            "Partition count cannot be zero"
                .into()
        );
    }

    let mut partitions = HashMap::new();

    for i in 0..partition_no {
        let file_name =
            format!(
                "{}_partition_{}.log",
                topic_name,
                i
            );

        std::fs::File::create(&file_name)
            .map_err(|e| {
                format!(
                    "Failed to create partition file '{}': {}",
                    file_name,
                    e
                )
            })?;

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

    Ok(partitions)
}

type WorkerRequest = (
    Arc<RwLock<server>>,
    Vec<u8>,
    Vec<u8>,
    Option<oneshot::Receiver<bool>>,
    oneshot::Sender<bool>,
    SocketAddr,
);

type PartitionWorkerRequest = (
    Arc<RwLock<server>>,
    Arc<RwLock<Partition>>,
    Vec<u8>,
    SocketAddr,
    
);

type ResponseRequest = (
    Arc<RwLock<server>>,
    SocketAddr,
    bool,
    Vec<u8>,
);

type ConsumerWorkerRequest = (
    Arc<server>,
    Arc<RwLock<Partition>>,
    SocketAddr,
);

#[derive(Debug)]
struct Partition {
    id: usize,
    file_name: String,
    consumers: Vec<Consumer>,
}

impl Partition {
    fn WriteTOFile(
        &self,
        value: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut file =
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.file_name)?;

        file.write_all(&value)?;
        file.write_all(b"\n")?;

        Ok(())
    }
}
struct PartitionWorkerTask {
        request: PartitionWorkerRequest,
        previous_signal: Option<oneshot::Receiver<()>>,
        completion_signal: oneshot::Sender<()>,
    }

#[derive(Debug)]
struct Consumer {
    consumer_addr: SocketAddr,
    start_point: usize,
    offset: usize,
}

#[derive(Hash, Eq, PartialEq)]
#[derive(Debug)]

struct Shard(usize);

impl server {
    fn new(
        shard_count: usize,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        if shard_count == 0 {
            return Err(
                "Shard count cannot be zero"
                    .into()
            );
        }

        let requesthandler_sender =
            server::RequestHandler();

        let partition_worker_pool =
            server::PartitionWorkerPool(4);

        let respnse_signla =
            ResponseWriter();

        Ok(Self {
            shard_map:
                CreateShardMap(shard_count),
            clients: HashMap::new(),
            shard_count,
            request_handler:
                requesthandler_sender,
            partition_worker_pool,
            response_writer_singal:
                respnse_signla,
        })
    }

    fn RequestHandler(
    ) -> mpsc::Sender<(
        Arc<RwLock<server>>,
        Vec<u8>,
        Vec<u8>,
        SocketAddr,
    )> {
        let (sender, mut receiver) =
            mpsc::channel::<(
                Arc<RwLock<server>>,
                Vec<u8>,
                Vec<u8>,
                SocketAddr,
            )>(1024);

        let worker_queue =
            server::WorkerPool(4);

        tokio::spawn(async move {
            let mut previous_receiver:
                Option<oneshot::Receiver<bool>> =
                None;

            while let Some((
                server,
                operation,
                payload,
                client_addr,
            )) = receiver.recv().await
            {
                let (
                    signal_sender,
                    signal_receiver,
                ) = oneshot::channel::<bool>();

                let previous =
                    previous_receiver.take();

                previous_receiver =
                    Some(signal_receiver);

                if let Err(e) =
                    worker_queue
                        .send((
                            server,
                            operation,
                            payload,
                            previous,
                            signal_sender,
                            client_addr,
                        ))
                        .await
                {
                    eprintln!(
                        "Failed to queue request into worker queue: {}",
                        e
                    );

                    break;
                }
            }
        });

        sender
    }

    fn WorkerPool(
        worker_count: usize,
    ) -> mpsc::Sender<WorkerRequest> {
        let (sender, mut receiver) =
            mpsc::channel::<WorkerRequest>(1024);

        if worker_count == 0 {
            eprintln!(
                "Worker pool cannot have zero workers"
            );

            return sender;
        }

        let mut worker_senders =
            Vec::new();

        for _ in 0..worker_count {
            let (
                worker_sender,
                mut worker_receiver,
            ) = mpsc::channel::<WorkerRequest>(256);

            worker_senders.push(worker_sender);

            tokio::spawn(async move {
                while let Some((
                    server,
                    operation,
                    payload,
                    previous_receiver,
                    signal_sender,
                    client_addr,
                )) = worker_receiver.recv().await
                {
                    let result =
                        server::HandleOperation(
                            &server,
                            operation,
                            payload,
                        )
                        .await;

                    match result {
                        Ok(Some((
                            value,
                            partition,
                        ))) => {
                            if let Some(
                                previous_receiver,
                            ) = previous_receiver
                            {
                                if let Err(e) =
                                    previous_receiver
                                        .await
                                {
                                    eprintln!(
                                        "Previous request signal failed: {}",
                                        e
                                    );
                                }
                            }

                            let partition_worker_pool =
                                {
                                    let server_guard =
                                        server.read().await;

                                    server_guard
                                        .partition_worker_pool
                                        .clone()
                                };

                            if let Err(e) =
                                partition_worker_pool
                                    .send((
                                        Arc::clone(&server),
                                        partition,
                                        value,
                                        client_addr,
                                    ))
                                    .await
                            {
                                eprintln!(
                                    "Failed to queue partition write: {}",
                                    e
                                );
                            }

                            let _ =
                                signal_sender.send(true);
                        }

                        // Ok(None) => {
                        //     let _ =
                        //         signal_sender.send(true);
                        // }

                        // Err(e) => {
                        //     eprintln!(
                        //         "HandleOperation failed: {}",
                        //         e
                        //     );

                        //     let _ =
                        //         signal_sender.send(true);
                        // }

                        Ok(None) => {
                            let response_writer_signal = {
                                let server_guard =
                                    server.read().await;

                                server_guard
                                    .response_writer_singal
                                    .clone()
                            };
                            if let Err(e) =
                                
                                response_writer_signal
                                    .send((
                                        Arc::clone(&server),
                                        client_addr,
                                        true,
                                        Vec::new(),
                                    ))
                                    .await
                            {
                                eprintln!(
                                    "Failed to queue response: {}",
                                    e
                                );
                            }

                            let _ =
                                signal_sender.send(true);
                        }

                        Err(e) => {
                            let response_writer_signal = {
                                let server_guard =
                                    server.read().await;

                                server_guard
                                    .response_writer_singal
                                    .clone()
                            };
                            eprintln!(
                                "HandleOperation failed: {}",
                                e
                            );

                            if let Err(e) =
                                response_writer_signal
                                    .send((
                                        Arc::clone(&server),
                                        client_addr,
                                        false,
                                        e.to_string().into_bytes(),
                                    ))
                                    .await
                            {
                                eprintln!(
                                    "Failed to queue error response: {}",
                                    e
                                );
                            }

                            let _ =
                                signal_sender.send(true);
                        }
                    }
                }
            });
        }

        tokio::spawn(async move {
            let mut next_worker = 0;

            while let Some(request) =
                receiver.recv().await
            {
                if worker_senders.is_empty() {
                    eprintln!(
                        "No workers available"
                    );
                    break;
                }

                if let Err(e) =
                    worker_senders[next_worker]
                        .send(request)
                        .await
                {
                    eprintln!(
                        "Failed to dispatch request to worker: {}",
                        e
                    );
                    break;
                }

                next_worker =
                    (next_worker + 1)
                        % worker_senders.len();
            }
        });

        sender
    }

    
    fn PartitionWorkerPool(
        worker_count: usize,
    ) -> mpsc::Sender<PartitionWorkerRequest> {
        let (sender, mut receiver) =
            mpsc::channel::<PartitionWorkerRequest>(
                1024,
            );

        if worker_count == 0 {
            eprintln!(
                "Partition worker pool cannot have zero workers"
            );

            return sender;
        }

        // let mut worker_senders =
        //     Vec::new();

        // for _ in 0..worker_count {
        //     let (
        //         worker_sender,
        //         mut worker_receiver,
        //     ) =
        //         mpsc::channel::<
        //             PartitionWorkerRequest,
        //         >(256);

        //     worker_senders.push(worker_sender);

        //     tokio::spawn(async move {
        //         while let Some((
        //             server,
        //             partition,
        //             value,
        //             client_addr,
        //         )) = worker_receiver.recv().await
        //         {
        //             let partition_guard =
        //                 partition.write().await;

        //             let response_writer_signal = {
        //                 let server_guard =
        //                     server.read().await;

        //                 server_guard
        //                     .response_writer_singal
        //                     .clone()
        //             };

        //             match partition_guard
        //                 .WriteTOFile(value)
        //             {
        //                 Ok(()) => {
        //                     if let Err(e) =
        //                         response_writer_signal
        //                             .send((
        //                                 Arc::clone(&server),
        //                                 client_addr,
        //                                 true,
        //                                 Vec::new(),
        //                             ))
        //                             .await
        //                     {
        //                         eprintln!(
        //                             "Failed to queue success response: {}",
        //                             e
        //                         );
        //                     }
        //                 }

        //                 Err(e) => {
        //                     let error_message =
        //                         e.to_string()
        //                             .into_bytes();

        //                     if let Err(send_err) =
        //                         response_writer_signal
        //                             .send((
        //                                 Arc::clone(&server),
        //                                 client_addr,
        //                                 false,
        //                                 error_message,
        //                             ))
        //                             .await
        //                     {
        //                         eprintln!(
        //                             "Failed to queue error response: {}",
        //                             send_err
        //                         );
        //                     }
        //                 }
        //             }

        //             println!(
        //                 "Worker writing to partition {} and file_name {}",
        //                 partition_guard.id,
        //                 partition_guard.file_name
        //             );
        //         }
        //     });
        // }

        let mut worker_senders = Vec::new();

for _ in 0..worker_count {
    let (
        worker_sender,
        mut worker_receiver,
    ) = mpsc::channel::<PartitionWorkerTask>(256);

    worker_senders.push(worker_sender);

    tokio::spawn(async move {
        while let Some(task) =
            worker_receiver.recv().await
        {
            let PartitionWorkerTask {
                request,
                previous_signal,
                completion_signal,
            } = task;

            let (
                server,
                partition,
                value,
                client_addr,
            ) = request;

            // println!(
            //     "Partition worker received request - waiting for previous signal"
            // );

            // Wait for the previous partition request
            // to finish its write.
            if let Some(previous_signal) =
                previous_signal
            {
                // println!(
                //     "Waiting for previous partition request..."
                // );

                if let Err(e) =
                    previous_signal.await
                {
                    eprintln!(
                        "Previous partition signal failed: {}",
                        e
                    );
                }

                // println!(
                //     "Previous partition request signal received"
                // );
            }

            // println!(
            //     "Partition worker starting write"
            // );

            let partition_guard =
                partition.write().await;

            let response_writer_signal = {
                let server_guard =
                    server.read().await;

                server_guard
                    .response_writer_singal
                    .clone()
            };

            match partition_guard
                .WriteTOFile(value)
            {
                Ok(()) => {
                    // println!(
                    //     "Partition write completed"
                    // );

                    if let Err(e) =
                        response_writer_signal
                            .send((
                                Arc::clone(&server),
                                client_addr,
                                true,
                                Vec::new(),
                            ))
                            .await
                    {
                        eprintln!(
                            "Failed to queue success response: {}",
                            e
                        );
                    }
                }

                Err(e) => {
                    let error_message =
                        e.to_string().into_bytes();

                    if let Err(send_err) =
                        response_writer_signal
                            .send((
                                Arc::clone(&server),
                                client_addr,
                                false,
                                error_message,
                            ))
                            .await
                    {
                        eprintln!(
                            "Failed to queue error response: {}",
                            send_err
                        );
                    }
                }
            }

            // println!(
            //     "Worker writing to partition {} and file_name {}",
            //     partition_guard.id,
            //     partition_guard.file_name
            // );

            // This request is now completely finished.
            // Wake the next request.
            // println!(
            //     "Sending partition completion signal"
            // );

            let _ =
                completion_signal.send(());
        }
    });
}

        // tokio::spawn(async move {
        //     let mut next_worker = 0;
        
        //     while let Some(request) =
        //         receiver.recv().await
        //     {
        //             println!("{:?}  ",request);

        //         if worker_senders.is_empty() {
        //             eprintln!(
        //                 "No partition workers available"
        //             );
        //             break;
        //         }

        //         if let Err(e) =
        //             worker_senders[next_worker]
        //                 .send(request,)
        //                 .await
        //         {
        //             eprintln!(
        //                 "Failed to dispatch partition request: {}",
        //                 e
        //             );
        //             break;
        //         }

        //         next_worker =(next_worker + 1) % worker_senders.len();
        //     }
        // });
        tokio::spawn(async move {
    let mut next_worker = 0;

    let mut previous_signal:
        Option<oneshot::Receiver<()>> = None;

    while let Some(request) =
        receiver.recv().await
    {
       

        if worker_senders.is_empty() {
            eprintln!(
                "No partition workers available"
            );
            break;
        }

        // Create the signal that THIS request will
        // send when its partition write is finished.
        let (
            completion_signal,
            completion_receiver,
        ) = oneshot::channel::<()>();

        let task = PartitionWorkerTask {
            request,
            previous_signal: previous_signal.take(),
            completion_signal,
        };

        // The receiver becomes the "previous signal"
        // for the NEXT request.
        previous_signal =
            Some(completion_receiver);

        if let Err(e) =
            worker_senders[next_worker]
                .send(task)
                .await
        {
            eprintln!(
                "Failed to dispatch partition request: {}",
                e
            );
            break;
        }

        next_worker =
            (next_worker + 1)
                % worker_senders.len();
    }
});
        sender
    }

    async fn HandleOperation(
        server: &Arc<RwLock<server>>,
        operation: Vec<u8>,
        payload: Vec<u8>,
    ) -> Result<
        Option<(
            Vec<u8>,
            Arc<RwLock<Partition>>,
        )>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let operation =
            String::from_utf8(operation)
                .map_err(|e| {
                    format!(
                        "Invalid operation UTF-8: {}",
                        e
                    )
                })?;

        match operation.trim() {
            "topic_insert" => {
                let (
                    topic_name_buf,
                    payload,
                ) = Simplify(payload)?;

                if payload.len() != 8 {
                    return Err(
                        format!(
                            "Invalid partition count payload: expected 8 bytes, got {}",
                            payload.len()
                        )
                        .into()
                    );
                }

                let partition_no =
                    u64::from_be_bytes(
                        payload
                            .try_into()
                            .map_err(|_| {
                                "Failed to parse partition count"
                            })?,
                    ) as usize;

                if partition_no == 0 {
                    return Err(
                        "Partition count cannot be zero"
                            .into()
                    );
                }

                let topic_map = {
                    let server_guard =
                        server.read().await;

                    let shard =
                        server_guard.GetShard(
                            &topic_name_buf,
                            server_guard
                                .shard_count,
                        );

                    let topic_map =
                        server_guard
                            .shard_map
                            .get(&shard)
                            .ok_or_else(|| {
                                format!(
                                    "Shard {} not found",
                                    shard.0
                                )
                            })?;

                    Arc::clone(topic_map)
                };

                let mut topic_map_guard =
                    topic_map.write().await;

                let topic =
                    topic::new(
                        &topic_name_buf,
                        partition_no,
                    )?;

                topic_map_guard.insert(
                    topic_name_buf,
                    topic,
                );

                Ok(None)
            }

            "topic_data_insert" => {
                let (
                    topic_name_buf,
                    payload,
                ) = Simplify(payload)?;

                let (
                    key_buf,
                    payload,
                ) = Simplify(payload)?;

                let (
                    value_buf,
                    _,
                ) = Simplify(payload)?;

                let partition = {
                    let server_guard =
                        server.read().await;

                    let shard =
                        server_guard.GetShard(
                            &topic_name_buf,
                            server_guard
                                .shard_count,
                        );

                    let topic_map =
                        server_guard
                            .shard_map
                            .get(&shard)
                            .ok_or_else(|| {
                                format!(
                                    "Shard {} not found",
                                    shard.0
                                )
                            })?;

                    let topic_map_guard =
                        topic_map.read().await;

                    let topic =
                        topic_map_guard
                            .get(&topic_name_buf)
                            .ok_or_else(|| {
                                format!(
                                    "Topic '{}' does not exist",
                                    String::from_utf8_lossy(
                                        &topic_name_buf
                                    )
                                )
                            })?;

                    let partition_no =
                        topic.partition_no;

                    if partition_no == 0 {
                        return Err(
                            "Topic has zero partitions"
                                .into()
                        );
                    }

                    let key_buf_hash =
                        server_guard
                            .GetHash(&key_buf)
                            as usize;

                    let partition_id =
                        key_buf_hash
                            % partition_no;

                    let partition =
                        topic
                            .partitions
                            .get(&partition_id)
                            .ok_or_else(|| {
                                format!(
                                    "Partition {} not found for topic",
                                    partition_id
                                )
                            })?;

                    Arc::clone(partition)
                };

                Ok(Some((
                    value_buf,
                    partition,
                )))
            }

            unknown => {
                Err(
                    format!(
                        "Unknown operation: {}",
                        unknown
                    )
                    .into()
                )
            }
        }
    }

    fn GetShard(
        &self,
        topic: &[u8],
        shard_count: usize,
    ) -> Shard {
        let mut hasher =
            DefaultHasher::new();

        topic.hash(&mut hasher);

        let hash = hasher.finish();

        Shard(
            (hash as usize)
                % shard_count
        )
    }

    fn GetHash(
        &self,
        data: &[u8],
    ) -> u64 {
        let mut hasher =
            DefaultHasher::new();

        data.hash(&mut hasher);

        hasher.finish()
    }
}

fn CreateShardMap(
    shard_count: usize,
) -> HashMap<
    Shard,
    Arc<RwLock<TopicMap>>,
> {
    let mut shards =
        HashMap::new();

    for i in 0..shard_count {
        shards.insert(
            Shard(i),
            Arc::new(
                RwLock::new(
                    TopicMap::new()
                )
            ),
        );
    }

    shards
}

fn ResponseWriter()
    -> mpsc::Sender<ResponseRequest>
{
    let (sender, mut receiver) =
        mpsc::channel::<ResponseRequest>(
            1024
        );

    tokio::spawn(async move {
        while let Some((
            server,
            client_addr,
            ack,
            response,
        )) = receiver.recv().await
        {
            let client = {
                let server_guard =
                    server.read().await;

                let Some(client) =
                    server_guard
                        .clients
                        .get(&client_addr)
                else {
                    eprintln!(
                        "Client not found: {}",
                        client_addr
                    );

                    continue;
                };

                Arc::clone(client)
            };

            let mut client_guard =
                client.write().await;

            let response_len =
                response.len() as u64;

            let mut output =
                Vec::with_capacity(
                    1 + 8 + response.len()
                );

            output.push(
                if ack { 1 } else { 0 }
            );

            output.extend_from_slice(
                &response_len
                    .to_be_bytes()
            );

            output.extend_from_slice(
                &response
            );

            if let Err(e) =
                client_guard
                    .write_all(&output)
                    .await
            {
                eprintln!(
                    "Failed to send response to {}: {}",
                    client_addr,
                    e
                );
            }
        }
    });

    sender
}

pub async fn Init(
    server_ready:
        tokio::sync::oneshot::Sender<()>,
) {
    let socket =
        match CreateSocket().await {
            Ok(socket) => socket,

            Err(e) => {
                eprintln!(
                    "Failed to create server socket: {}",
                    e
                );

                return;
            }
        };

    let shard_count: usize = 10;

    let server =
        match server::new(shard_count) {
            Ok(server) => {
                Arc::new(
                    RwLock::new(server)
                )
            }

            Err(e) => {
                eprintln!(
                    "Failed to initialize server: {}",
                    e
                );

                return;
            }
        };

    if server_ready.send(()).is_err() {
        eprintln!(
            "Failed to signal server readiness"
        );
    }

    println!("Server started");

    loop {
        let (
            stream,
            client_addr,
        ) = match socket.accept().await {
            Ok(connection) =>
                connection,

            Err(e) => {
                eprintln!(
                    "Failed to accept connection: {}",
                    e
                );

                continue;
            }
        };

        println!(
            "Client connected: {}",
            client_addr
        );

        let server =
            Arc::clone(&server);

        tokio::spawn(async move {
            let (
                mut reader,
                writer,
            ) = stream.into_split();

            {
                let mut server_guard =
                    server.write().await;

                let writer_stream =
                    Arc::new(
                        RwLock::new(writer)
                    );

                server_guard
                    .clients
                    .insert(
                        client_addr,
                        writer_stream,
                    );
            }

            let server =
                Arc::clone(&server);

            'connection: loop {
                let mut count_buf =
                    [0u8; 8];

                if let Err(e) =
                    reader
                        .read_exact(
                            &mut count_buf
                        )
                        .await
                {
                    eprintln!(
                        "Client {} disconnected: {}",
                        client_addr,
                        e
                    );

                    break 'connection;
                }

                let request_count =
                    u64::from_be_bytes(
                        count_buf
                    ) as usize;

                if request_count == 0 {
                    eprintln!(
                        "Client {} sent empty batch",
                        client_addr
                    );

                    continue;
                }

                let mut allreq_buf_len =
                    [0u8; 8];

                if let Err(e) =
                    reader
                        .read_exact(
                            &mut allreq_buf_len
                        )
                        .await
                {
                    eprintln!(
                        "Failed reading batch length from {}: {}",
                        client_addr,
                        e
                    );

                    break 'connection;
                }

                let len =
                    u64::from_be_bytes(
                        allreq_buf_len
                    ) as usize;

                let mut all_req_buf =
                    vec![0u8; len];

                if let Err(e) =
                    reader
                        .read_exact(
                            &mut all_req_buf
                        )
                        .await
                {
                    eprintln!(
                        "Failed reading batch data from {}: {}",
                        client_addr,
                        e
                    );

                    break 'connection;
                }

                let mut remaining_buf =
                    all_req_buf;

                for request_index
                    in 0..request_count
                {
                    let (
                        request,
                        remaining,
                    ) = match Simplify(
                        remaining_buf
                    ) {
                        Ok(result) =>
                            result,

                        Err(e) => {
                            eprintln!(
                                "Failed to parse request {} from {}: {}",
                                request_index,
                                client_addr,
                                e
                            );

                            break 'connection;
                        }
                    };

                    remaining_buf =
                        remaining;

                    let (
                        operation,
                        payload,
                    ) = match Simplify(
                        request
                    ) {
                        Ok(result) =>
                            result,

                        Err(e) => {
                            eprintln!(
                                "Failed to parse operation/payload for request {} from {}: {}",
                                request_index,
                                client_addr,
                                e
                            );

                            break 'connection;
                        }
                    };

                    let request_handler = {
                        let server_guard =
                            server.read().await;

                        server_guard
                            .request_handler
                            .clone()
                    };

                    if let Err(e) =
                        request_handler
                            .send((
                                Arc::clone(
                                    &server
                                ),
                                operation,
                                payload,
                                client_addr,
                            ))
                            .await
                    {
                        eprintln!(
                            "Failed to queue request from {}: {}",
                            client_addr,
                            e
                        );

                        break 'connection;
                    }
                }

                if !remaining_buf.is_empty() {
                    eprintln!(
                        "Batch from {} contained {} unconsumed bytes",
                        client_addr,
                        remaining_buf.len()
                    );
                }
            }

            {
                let mut server_guard =
                    server.write().await;

                server_guard
                    .clients
                    .remove(
                        &client_addr
                    );
            }

            println!(
                "Client {} connection handler stopped",
                client_addr
            );
        });
    }
}

fn Simplify(
    buf: Vec<u8>,
) -> Result<
    (Vec<u8>, Vec<u8>),
    Box<dyn Error + Send + Sync>,
> {
    if buf.len() < 8 {
        return Err(
            format!(
                "Buffer too short: {} bytes, expected at least 8",
                buf.len()
            )
            .into()
        );
    }

    let len =
        u64::from_be_bytes(
            buf[..8]
                .try_into()
                .map_err(|_| {
                    "Failed to read length"
                })?,
        ) as usize;

    let end =
        8usize
            .checked_add(len)
            .ok_or(
                "Length overflow"
            )?;

    if end > buf.len() {
        return Err(
            format!(
                "Invalid buffer length: declared {}, available {}",
                len,
                buf.len().saturating_sub(8)
            )
            .into()
        );
    }

    let value =
        buf[8..end].to_vec();

    let remaining =
        buf[end..].to_vec();

    Ok((
        value,
        remaining,
    ))
}

async fn CreateSocket()
    -> Result<TcpListener, Box<dyn Error>>
{
    let addr =
        std::env::var("server_addr")
            .map_err(|_| {
                "Environment variable 'server_addr' not defined"
            })?;

    let socket =
        TcpListener::bind(&addr)
            .await?;

    Ok(socket)
}