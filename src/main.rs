use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fmt::Error;
use std::sync::{Arc, Mutex};
use std::{io, thread};
use resp::Value;
use tokio::net::{TcpListener, TcpStream};
use anyhow::Result;
use tokio::time::Instant;
use clap::Parser;
use nanoid::nanoid;
use uuid::Uuid;
use thiserror::Error;
use db::KeyValueData;
use crate::connect::connect_to_master;
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
    pub master_replid: Uuid,
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

        return Ok(RedisServer {
            is_master,
            connected_slaves: 0,
            master_replid: Uuid::new_v4(),
            master_repl_offset: 0,
            master_nanoid: nanoid!(),
            master_host,
            master_port,
            self_port,
        })
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
        Ok(s) => Arc::new(s),
        Err(e) => {
            println!("Unable to initialize Redis server: {e}");
            return;
        }
    };



    let addr = format!("0.0.0.0:{}", server_info.self_port.to_string());

    // let args = Args::parse();
    println!("Listening on address {addr}");

    let listener = TcpListener::bind(addr).await.unwrap();
    let data: Arc<Mutex<HashMap<String, KeyValueData>>> = Arc::new(Mutex::new(HashMap::new()));
    let exp_heap: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>> = Arc::new(Mutex::new(BinaryHeap::new()));

    let data_clean = Arc::clone(&data);
    let exp_clean = Arc::clone(&exp_heap);
    let _ = thread::spawn(move || key_expiry_thread(data_clean, exp_clean, EXPIRY_LOOP_TIME));

    if !server_info.is_master {
        let mut stream_to_master = connect_to_master(server_info.master_host.clone(), server_info.master_port.clone()).await;

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

async fn handle_conn(stream: TcpStream, server_info_clone: Arc<RedisServer>, data1: Arc<Mutex<HashMap<String, KeyValueData>>>, exp_heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>) {
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
                }
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

async fn handle_conn_to_master(stream_to_master: TcpStream, server_info_clone: Arc<RedisServer>, data1: Arc<Mutex<HashMap<String, KeyValueData>>>, exp_heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>) {
    let mut handler = resp::RespHandler::new(stream_to_master);
    println!("Connection to master handled, trying to send HandShake.");
    let mut handshake = Value::Array(vec![Value::BulkString("PING".to_string())]);
    handler.write_value(handshake).await.unwrap();

    loop {
        let value = handler.read_value().await.unwrap();
        println!("Got value {:?}", value);
    }

}


fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::SimpleString(_s) => {
            Err(anyhow::anyhow!("SimpleString value response is todo!"))
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

fn unpack_array(value: Value) -> Result<Vec<Value>> {
    match value {
        Value::Array(a) => Ok(a),
        _ => Err(anyhow::anyhow!("Expected command to be an array"))
    }
}

fn unpack_simple_str(value: Value) -> Result<String> {
    match value {
        Value::SimpleString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a simple string"))
    }
}

