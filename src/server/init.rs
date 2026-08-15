use std::{error::Error, hash::DefaultHasher, net::SocketAddr};

use tokio::{io::AsyncReadExt, net::{TcpListener, TcpStream}};
use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use std::hash::{Hash, Hasher};



struct server{
    shard_map:HashMap<Shard,TopicMap>,
    clients:HashMap<SocketAddr,TcpStream>,
    shard_count:usize
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
    fn new(topic_name: Vec<u8>, partition_no: usize) -> Self {
        let partitions = CreatePartitions(&topic_name, partition_no);

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

struct Partition {
    id: usize,
    file_name: String,
    consumers:Vec<Consumer>
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
        Self{
            shard_map:CreateShardMap(shard_count.clone()),
            clients:HashMap::new(),
            shard_count:shard_count
        }
    }

   

    async fn HandleOperation(&mut self,operation:Vec<u8>,payload: Vec<u8>){
        let operation=String::from_utf8(operation).unwrap();
    
        match operation.trim() {
            "topic_insert"=>{
                let (topic_buf,payload)=Simplify(payload);

                let (partition_no,_)=Simplify(payload);

                let shard=self.GetShard(&topic_buf, self.shard_count);

                let topic_map=TopicMap::new();

                // let topic=
                
                // let a=self.shard_map.insert(shard, topic_map);

                

            }
            "topic_data_insert"=>{
                let (topic_name_buf,payload)=Simplify(payload);

                let (key_buf,payload)=Simplify(payload);

                let (value_buf,payload)=Simplify(payload);


                let shard=self.GetShard(&topic_name_buf, self.shard_count);

                let topic_map=self.shard_map.get(&shard).unwrap();

                let topic =topic_map.get(&topic_name_buf).unwrap();

                let key_buf_hash=self.GetHash(&key_buf) as usize;

                let partition= topic.partitions.get(&key_buf_hash).unwrap();

                let partition_guard=partition.write().await;

                //writeto partiton log file 
            }
            _=>{

            }
        }
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

fn CreateShardMap(shard_count:usize)->HashMap<Shard, TopicMap>{
    let mut shards = HashMap::new();
    for i in 0..shard_count{
        shards.insert(
            Shard(i),
            TopicMap {
                map: HashMap::new(),
            },
        );
    }
    shards
}


async fn Init(){
    let socket=CreateSocket().await.unwrap();
    let shard_count:usize=10;    
    let server=Arc::new(server::new(shard_count));

    loop {
        let (mut stream,client_addr)=socket.accept().await.unwrap();

        let server_client=Arc::clone(&server);
        
        let mut buf_len=[0u8;8];

        stream.read_exact(&mut buf_len).await.unwrap();

        let len = u64::from_be_bytes(buf_len);

        let mut buf = vec![0u8; len as usize];

        stream.read_exact(&mut buf).await.unwrap();

        let mut op_len=[0u8;8];



        let (operation,payload)=Simplify(buf);

        //tokio new swpan taksk
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