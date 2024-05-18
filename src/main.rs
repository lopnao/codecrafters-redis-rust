use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};
use std::{io, thread};
use resp::Value;
use tokio::net::{TcpListener, TcpStream};
use anyhow::Result;
use tokio::time::Instant;
use clap::Parser;
use nanoid::nanoid;
use rand::distributions::Alphanumeric;
use rand::Rng;
use thiserror::Error;
use db::KeyValueData;
use crate::connect::{configure_replica, connect_to_master, psync};
use crate::db::{data_get, data_set, key_expiry_thread, server_info};

mod resp;
mod db;
mod structs;
mod connect;

const EXPIRY_LOOP_TIME: u64 = 1; // 1 milli seconds

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("`{0}`")]
    Redaction(String),
    #[error("Bad replicaof argument")]
    BadReplicaOf,
    #[error("Bad replicaof port")]
    BadReplicaPort,
    #[error("Server Initialisation Error")]
    ServerInit,
    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader {
        expected: String,
        found: String,
    },
    #[error("unknown server error")]
    Unknown,
}

#[derive(Error, Debug)]
pub enum SlaveError {
    #[error("`{0}`")]
    Redaction(String),
    #[error("Connection to master error")]
    Connection(#[from] io::Error),
    #[error("Connection to master error : No Host Provided")]
    NoHost,
    #[error("unknown server error")]
    Unknown,
}

#[allow(dead_code)]
struct RedisServer {
    is_master: bool,
    pub connected_slaves: u32,
    pub master_replid: String,
    pub master_repl_offset: u32,
    pub master_nanoid: String,
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub self_port: u16,
}

impl RedisServer {

    fn init() -> Result<Self, ServerError> {
        let mut self_port = 6379;
        let args = Args::parse();
        if let Some(port) = args.port {
            self_port = port;
        }

        let mut master_host: Option<String> = None;
        let mut master_port: Option<u16> = None;
        let mut is_master = true;

        if let Some(replica) = args.replicaof {
            is_master = false;
            let mut replica_info: Vec<&str> = replica.split_whitespace().collect();
            if replica_info.len() != 2 {
                return Err(ServerError::BadReplicaOf);
            }

            master_host = Some(replica_info.remove(0).to_string());
            match replica_info.remove(0).parse::<u16>() {
                Ok(port) => master_port = Some(port),
                Err(_) => return Err(ServerError::BadReplicaPort),
            };
        }

        let replid: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();
        println!(" Creating replid : {}", replid);

        return Ok(RedisServer {
            is_master,
            connected_slaves: 0,
            master_replid: replid,
            master_repl_offset: 0,
            master_nanoid: nanoid!(),
            master_host,
            master_port,
            self_port,
        })
    }

    fn change_replid(&mut self, new_replid: String) {
        self.master_replid = new_replid;
    }

}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    let server_info = match RedisServer::init() {
        Ok(s) => Arc::new(Mutex::new(s)),
        Err(e) => {
            println!("Unable to initialize Redis server: {e}");
            return;
        }
    };



    let addr = {
        let temp_port = server_info.lock().unwrap().self_port.to_string();
        format!("0.0.0.0:{}", temp_port)
    };

    // let args = Args::parse();
    println!("Listening on address {addr}");

    let listener = TcpListener::bind(addr).await.unwrap();
    let data: Arc<Mutex<HashMap<String, KeyValueData>>> = Arc::new(Mutex::new(HashMap::new()));
    let exp_heap: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>> = Arc::new(Mutex::new(BinaryHeap::new()));

    let data_clean = Arc::clone(&data);
    let exp_clean = Arc::clone(&exp_heap);
    let _ = thread::spawn(move || key_expiry_thread(data_clean, exp_clean, EXPIRY_LOOP_TIME));

    let is_slave = {
        !server_info.lock().unwrap().is_master
    };

    if is_slave {
        let master_host = { server_info.lock().unwrap().master_host.clone() };
        let master_port = { server_info.lock().unwrap().master_port.clone() };
        let stream_to_master = connect_to_master(master_host, master_port).await;

        match stream_to_master {
            Ok(stream_to_master) => {
                let data1 = Arc::clone(&data);
                let exp_heap1 = Arc::clone(&exp_heap);
                let server_info_clone = server_info.clone();
                tokio::spawn(async move {
                    handle_conn_to_master(stream_to_master, server_info_clone, data1, exp_heap1).await
                });
            }
            Err(e) => { println!("error connecting to master: {}", e); }
        }
    }




    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let data1 = Arc::clone(&data);
                let exp_heap1 = Arc::clone(&exp_heap);
                let server_info_clone = server_info.clone();
                tokio::spawn(async move {
                    handle_conn(stream, server_info_clone, data1, exp_heap1).await
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }

    }
}

// #[derive(Debug, Default, Parser)]
// #[command(author, version, about, long_about = None)]
// struct Args {
//     #[clap(default_value_t=DEFAULT_LISTENING_PORT, short, long)]
//     port: u16,
// }

async fn handle_conn(stream: TcpStream, server_info_clone: Arc<Mutex<RedisServer>>, data1: Arc<Mutex<HashMap<String, KeyValueData>>>, exp_heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>) {
    let mut handler = resp::RespHandler::new(stream);
    println!("Starting read loop");

    loop {
        let value = handler.read_value().await.unwrap();
        println!("Got value {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.to_ascii_lowercase().as_str() {
                "ping"  => Value::SimpleString("PONG".to_string()),
                "echo"  => args.first().unwrap().clone(),
                "set"   => {
                    let data2 = Arc::clone(&data1);
                    let exp_heap2 = Arc::clone(&exp_heap1);
                    data_set(args, data2, exp_heap2)
                },
                "get"   => {
                    let data2 = Arc::clone(&data1);
                    data_get(args, data2)

                },
                "info"  => {
                    server_info(server_info_clone.clone(), args)
                },
                "replconf" => {
                    configure_replica(args)
                },
                "psync" => {
                    psync(args, server_info_clone.clone())
                },
                c => panic!("Cannot handle command {}", c),
            }
        } else {
            break;
        };
        println!("Sending value {:?}", response);
        println!("Serialized = {:?}", response.clone().serialize());

        handler.write_value(response).await.unwrap();

    }
}

#[allow(unused_variables)]
async fn handle_conn_to_master(stream_to_master: TcpStream, server_info_clone: Arc<Mutex<RedisServer>>, data1: Arc<Mutex<HashMap<String, KeyValueData>>>, exp_heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>) {
    let self_port = { server_info_clone.lock().unwrap().self_port.clone() };
    let mut handler = resp::RespHandler::new(stream_to_master);
    println!("Connection to master handled, trying to send HandShake.");
    let mut handshake = Value::Array(vec![Value::BulkString("PING".to_string())]);
    handler.write_value(handshake).await.unwrap();
    let value = handler.read_value().await.unwrap();
    if let Some(value) = value {
        if value == Value::SimpleString("PONG".to_string()) {
            let cmd = vec![
                Value::BulkString("REPLCONF".to_string()),
                Value::BulkString("listening-port".to_string()),
                Value::BulkString(format!("{}", self_port)),
            ];
            handshake = Value::Array(cmd);
            handler.write_value(handshake).await.unwrap();
        }
    }
    let value = handler.read_value().await.unwrap();
    if value == Some(Value::SimpleString("OK".to_string())) {
        let cmd = vec![
            Value::BulkString("REPLCONF".to_string()),
            Value::BulkString("capa".to_string()),
            Value::BulkString("psync2".to_string()),
        ];
        handshake = Value::Array(cmd);
        handler.write_value(handshake).await.unwrap();
    }
    let value = handler.read_value().await.unwrap();
    if value == Some(Value::SimpleString("OK".to_string())) {
        let cmd = vec![
            Value::BulkString("PSYNC".to_string()),
            Value::BulkString("?".to_string()),
            Value::BulkString("-1".to_string()),
        ];
        handshake = Value::Array(cmd);
        handler.write_value(handshake).await.unwrap();
    }
    if let Some(value) = handler.read_value().await.unwrap() {
        let (command, args) = extract_command(value).unwrap();
        match command.to_ascii_lowercase().as_str() {
            "fullresync"      => {
                if let Value::SimpleString(s) = args[0].clone() {
                    {
                        println!("Changed internal master_id to : {}", s);
                        let _temp = server_info_clone.lock().unwrap().change_replid(s);
                    };
                }

            },
            c     => panic!("Cannot handle command {} during handshake", c),
        }
    }




    loop {
        let value = handler.read_value().await.unwrap();
        println!("Got value ICI {:?}", value);
    }

}


fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::SimpleString(s) => {
            let v: Vec<&str> = s.split_whitespace().collect();
            let cmd = v[0];
            let args: Vec<Value> = v.iter().skip(1).map(|s| Value::SimpleString(format!("{}", s))).collect();
            Ok((cmd.to_string(), args))
        }
        Value::BulkString(_s) => {
            Err(anyhow::anyhow!("BulkString value response is todo!"))
        }
        Value::NullBulkString() => {
            Err(anyhow::anyhow!("NullBulkString value response is todo!"))
        }
        Value::Array(a) => {
            Ok((unpack_bulk_str(a[0].clone())?, a[1..].to_vec()))
        }
        Value::ArrayBulkString(a) => {
            Ok((unpack_bulk_str(a.first().unwrap().clone())?, a.into_iter().skip(1).collect()))
        }
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string"))
    }
}

#[allow(dead_code)]
fn unpack_array(value: Value) -> Result<Vec<Value>> {
    match value {
        Value::Array(a) => Ok(a),
        _ => Err(anyhow::anyhow!("Expected command to be an array"))
    }
}

#[allow(dead_code)]
fn unpack_simple_str(value: Value) -> Result<String> {
    match value {
        Value::SimpleString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a simple string"))
    }
}

