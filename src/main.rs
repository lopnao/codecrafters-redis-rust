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
use tokio::fs::metadata;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::sync::mpsc::{Receiver, Sender};
use commands::server_info;
use db::KeyValueData;
use crate::commands::{cmd_keys, cmd_xadd, cmd_xrange, cmd_xread, get_type, server_config, wait_or_replicas};
use crate::connect::{configure_replica, connect_to_master, psync, send_rdb_base64_to_hex};
use crate::db::{data_get, data_incr, data_set, data_set_from_rdb, key_expiry_thread, StreamDB};
use crate::rdb::read_rdb_file;
use crate::resp::RespHandler;
use crate::resp::CommandRedis::{GoodAckFromReplica, UpdateReplicasCount};
use crate::resp::Value::NullBulkString;

mod resp;
mod db;
mod structs;
mod connect;
mod commands;
mod rdb;

const _TIMEOUT_FROM_CHANNEL: u64 = 350;
const EXPIRY_LOOP_TIME: u64 = 50; // 50 milli seconds
const EMPTY_RDB_FILE: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("`{0}`")]
    Redaction(String),
    #[error("Error with Handshake step `{0}` `{1}`")]
    HandshakeError(usize, String),
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
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub self_port: u16,
    pub self_nanoid: String,
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    pub data: Arc<Mutex<HashMap<String, KeyValueData>>>,
    pub exp_heap: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>,
    pub stream_db: Arc<Mutex<StreamDB>>,
}

impl RedisServer {

    async fn init() -> Result<Self, ServerError> {
        let mut self_port = 6379;
        let args = Args::parse();
        if let Some(port) = args.port {
            self_port = port;
        }

        let mut master_host: Option<String> = None;
        let mut master_port: Option<u16> = None;
        let mut is_master = true;
        let mut dir: Option<String> = None;
        let mut dbfilename: Option<String> = None;

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

        if let Some(arg_dir) = args.dir {
            dir = Some(arg_dir);
        }

        if let Some(arg_dbfilename) = args.dbfilename {
            dbfilename = Some(arg_dbfilename);
        }

        let data: Arc<Mutex<HashMap<String, KeyValueData>>> = Arc::new(Mutex::new(HashMap::new()));
        let exp_heap: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>> = Arc::new(Mutex::new(BinaryHeap::new()));
        let stream_db: Arc<Mutex<StreamDB>> = Arc::new(Mutex::new(StreamDB::init()));

        if !dir.is_none() & !dbfilename.is_none() {
            let path_to_db_filename = dir.clone().unwrap() + "/" + &dbfilename.clone().unwrap();
            println!("Trying to open file: {:?}", path_to_db_filename);
            if metadata(path_to_db_filename.clone()).await.is_ok() {
                let (data_read, data_size) = read_rdb_file(&path_to_db_filename).await.unwrap();
                println!("dbfile has been read, size of {:?} !", data_size);
                println!("dbfile : {:?}", data_read);
                // Process la RDBStruct en Hashmap
                // avec la fn get_map()
                let data_clone = data.clone();
                let exp_heap_clone = exp_heap.clone();
                data_set_from_rdb(data_read, data_clone, exp_heap_clone);
            } else { println!("Can't open the file : {:?}. Starting fresh.", path_to_db_filename.clone()); }
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
            master_host,
            master_port,
            self_port,
            self_nanoid: nanoid!(),
            dir,
            dbfilename,
            data,
            exp_heap,
            stream_db
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
    #[arg(long)]
    dir: Option<String>,
    #[arg(long)]
    dbfilename: Option<String>,
}

#[tokio::main]
async fn main() {
    let server_info = match RedisServer::init().await {
        Ok(s) => Arc::new(Mutex::new(s)),
        Err(e) => {
            println!("Unable to initialize Redis server: {e}");
            return;
        }
    };
    let (broadcast_sender, broadcast_receiver) = broadcast::channel::<Value>(50);
    let (slave_tx, master_rx) = mpsc::channel::<Value>(50);
    let (_watch_tx, watch_rx) = watch::channel(broadcast_receiver.resubscribe());
    let (watch_replicas_count_tx, watch_replicas_count_rx) = watch::channel(0);

    let addr = {
        let temp_port = server_info.lock().unwrap().self_port.to_string();
        format!("0.0.0.0:{}", temp_port)
    };

    println!("Listening on address {addr}");

    let listener = TcpListener::bind(addr).await.unwrap();
    let server_info_clone = server_info.clone();

    let data: Arc<Mutex<HashMap<String, KeyValueData>>> = {
        server_info_clone.lock().unwrap().data.clone()
    };
    let exp_heap: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>> = {
        server_info_clone.lock().unwrap().exp_heap.clone()
    };
    let stream_db: Arc<Mutex<StreamDB>> = {
        server_info_clone.lock().unwrap().stream_db.clone()
    };

    // Spawning the cleaning thread for the data with expiry
    let _cleaning_thread = thread::spawn({
        let data_clean = Arc::clone(&data);
        let exp_clean = Arc::clone(&exp_heap);

        move || {
            key_expiry_thread(data_clean, exp_clean, EXPIRY_LOOP_TIME)
        }
    });

    // Setting the slave status if true
    let is_slave = {
        !server_info.lock().unwrap().is_master
    };

    // Connecting to master node if slave status is true
    if is_slave {
        let master_host = { server_info.lock().unwrap().master_host.clone() };
        let master_port = { server_info.lock().unwrap().master_port.clone() };
        let stream_to_master = connect_to_master(master_host, master_port).await;

        match stream_to_master {
            Ok(stream_to_master) => {
                let server_info_clone = server_info.clone();
                let data1 = Arc::clone(&data);
                let exp_heap1 = Arc::clone(&exp_heap);
                tokio::spawn(async move {
                    handle_conn_to_master(stream_to_master, server_info_clone, data1, exp_heap1).await
                });
            }
            Err(e) => { println!("error connecting to master: {}", e); }
        }
    }

    // Spawning a thread on master node to propagate Values to replica servers
    if !is_slave {
        tokio::spawn(async move {
            propagate_to_replicas(master_rx, broadcast_sender, watch_replicas_count_tx).await
        });
    }

    // Main loop
    loop {

        // Spawning a thread with a new stream for each new connection
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");

                tokio::spawn({
                    let server_info_clone = server_info.clone();
                    let data1 = Arc::clone(&data);
                    let stream_data = stream_db.clone();
                    let exp_heap1 = Arc::clone(&exp_heap);
                    let slave_tx_clone = slave_tx.clone();
                    let watch_rx_clone = watch_rx.clone();
                    let watch_replicas_count_rx_clone = watch_replicas_count_rx.clone();
                    async move {
                        handle_conn(stream, server_info_clone, data1, stream_data, exp_heap1, slave_tx_clone, watch_rx_clone, watch_replicas_count_rx_clone).await
                    }
                 });

            }
            Err(e) => {
                println!("error: {}", e);
            }
        }

    }
}

async fn propagate_to_replicas(mut master_receiver: Receiver<Value>,
                               broadcast_sender: broadcast::Sender<Value>,
                               watch_replicas_count_tx: watch::Sender<usize>) {

    // Create FnOnce(&mut usize) to modify and notify only if change
    let mut good_ack = 0;
    let count_add = | count: &mut usize | {
        *count += 1;
        true
    };
    let count_reset = | count: &mut usize | {
        if *count != 0 {
            *count = 0;
            return true;
        }
        false
    };


    loop {

        while let Some(value_to_propagate) = master_receiver.recv().await {
            if let Value::SimpleCommand(command) = &value_to_propagate {
                match command {
                    UpdateReplicasCount => {
                        broadcast_sender.send(Value::SimpleCommand(UpdateReplicasCount)).unwrap();
                    },
                    GoodAckFromReplica => {
                        good_ack += 1;
                        //println!("Updating replicas_good_count : incrementing +1");
                        watch_replicas_count_tx.send_if_modified(count_add);
                        //println!("on a maintenant count_watch = {:?}", watch_replicas_count_tx.borrow().clone());
                    }
                    _ => {},
                }
            } else {
                good_ack = 0;
                println!("Propagating : Value = {:?}", value_to_propagate.clone());
                broadcast_sender.send(value_to_propagate).unwrap();

                println!("Updating replicas_good_count with value of {}", good_ack);
                // Updating the count with good ack only replicas
                watch_replicas_count_tx.send_if_modified(count_reset);

            }
        }
    }
}

async fn handle_conn(stream: TcpStream, server_info_clone: Arc<Mutex<RedisServer>>,
                     data1: Arc<Mutex<HashMap<String, KeyValueData>>>,
                     stream_db_clone: Arc<Mutex<StreamDB>>,
                     exp_heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>,
                     slave_tx: Sender<Value>, watch_rx: watch::Receiver<broadcast::Receiver<Value>>,
                     watch_replicas_count_rx_clone: watch::Receiver<usize>) {
    let mut handler = RespHandler::new(stream);
    let mut to_replicate = false;
    let mut multi_bool = false;
    let mut commands_queue: Vec<Value> = vec![];
    let mut master_offset = (0_usize, 0_usize);
    let ack_command_to_check = Value::Array(vec![
        Value::BulkString("REPLCONF".to_string()),
        Value::BulkString("GETACK".to_string()),
        Value::BulkString("*".to_string()),
    ]);
    println!("Starting read loop");

    loop {
        let value = handler.read_value().await.unwrap();
        println!("Got value {:?}", value);


        let response = if let Some(v) = value {
            let value_to_propagate = v.clone();
            let (command, args) = extract_command(v).unwrap();
            match command.to_ascii_lowercase().as_str() {
                "ping"  => Value::SimpleString("PONG".to_string()),
                "echo"  => args.first().unwrap().clone(),
                "multi" => {
                    Value::SimpleString("OK".to_string())
                },
                "set"   => {
                    slave_tx.send(value_to_propagate).await.unwrap();
                    let data2 = Arc::clone(&data1);
                    let exp_heap2 = Arc::clone(&exp_heap1);
                    data_set(args, data2, exp_heap2)
                },
                "incr"   => {
                    slave_tx.send(value_to_propagate).await.unwrap();
                    let data2 = Arc::clone(&data1);
                    data_incr(args, data2)
                },
                "get"   => {
                    let data2 = Arc::clone(&data1);
                    data_get(args, data2)
                },
                "type"   => {
                    let data2 = Arc::clone(&data1);
                    let stream_data = stream_db_clone.clone();
                    get_type(args, data2, stream_data)
                },
                "xadd"  => {
                    let stream_data = stream_db_clone.clone();
                    if let Ok(value) = cmd_xadd(args, stream_data) {
                        value
                    } else { NullBulkString() }
                },
                "xrange"    => {
                    let stream_data = stream_db_clone.clone();
                    if let Ok(response_value) = cmd_xrange(args, stream_data) {
                        response_value
                    } else { Value::NullBulkString() }
                },
                "xread"    => {
                    let stream_data = stream_db_clone.clone();
                    if let Ok(response_value) = cmd_xread(args, stream_data).await {
                        response_value
                    } else { Value::NullBulkString() }
                },
                "info"  => {
                    server_info(server_info_clone.clone(), args)
                },
                "replconf" => {
                    configure_replica(args, 0)
                },
                "psync" => {
                    handler.write_value(psync(args, server_info_clone.clone())).await.unwrap();
                    // Sending RDB File
                    println!("[INFO] - Sending RDB File to Replica");
                    handler.write_value(send_rdb_base64_to_hex(EMPTY_RDB_FILE)).await.unwrap();
                    to_replicate = true;
                    slave_tx.send(Value::SimpleCommand(GoodAckFromReplica)).await.unwrap();
                    Value::SimpleString("OK".to_string())
                },
                "wait" => {
                    let watch_replicas_count_rx_clone = watch_replicas_count_rx_clone.clone();
                    slave_tx.send(Value::SimpleCommand(UpdateReplicasCount)).await.unwrap();
                    let res = wait_or_replicas(args, watch_replicas_count_rx_clone).await;
                    res
                },
                "config"=> {
                    server_config(args, server_info_clone.clone())
                },
                "keys"  => {
                    let data1_clone = data1.clone();
                    cmd_keys(args, data1_clone)
                },
                c => panic!("Cannot handle command {}", c),

            }
        } else {
            break;
        };
        if to_replicate {
            // Jumping to the propagation loop
            break;
        } else {
            println!("Sending value {:?}", response);
            println!("Serialized = {:?}", response.clone().serialize());

            handler.write_value(response).await.unwrap();
        }
    }
    // Propagate to Replica if the connection is to a Replica node :
    if to_replicate {
        let mut broadcast_receiver = { watch_rx.borrow().resubscribe() };

        loop {
            tokio::select! {
                Ok(v) = broadcast_receiver.recv() => {
                    match v {
                        Value::ArrayBulkString(v) => {
                            master_offset.0 += master_offset.1;
                            master_offset.1 = 0;
                            master_offset.0 += handler.write_value_and_count(Value::ArrayBulkString(v).deserialize_bulkstring()).await.unwrap();


                        },
                        Value::SimpleCommand(UpdateReplicasCount) => {
                            master_offset.1 = handler.
                                write_value_and_count(ack_command_to_check.clone()).await.unwrap();
                            //println!("Envoyé le ACK : en attente de réponse avec valeur = {:?}", master_offset);
                        }
                        a => {
                            println!("ICI : a = {:?}", a);
                        },
                    }
                }

                Ok(Some(val)) = handler.read_value() => {
                    println!("val = {:?}", val);
                    match val {
                        Value::ArrayBulkString(v) | Value::Array(v) => {
                            println!("ICI v = {:?}", v);
                            if let Some(Value::BulkString(s1)) = v.get(1) {
                                if s1.to_ascii_lowercase().as_str() == "ack" {
                                    if let Some(Value::BulkString(s2)) =v.get(2) {
                                        if let Ok(replica_offset) = s2.parse::<usize>() {
                                            println!("Apres parse, on a replica_offset = {:?}", replica_offset);
                                            if replica_offset == master_offset.0 {
                                                println!("Received the good offset from replica");
                                                slave_tx.send(Value::SimpleCommand(GoodAckFromReplica)).await.unwrap();
                                            } else {
                                                println!("Received the bad offset from replica : replica_offset = {} // master_offset = {}", replica_offset, master_offset.0);
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        vallllue => {
                            println!("ICI valllllue = {:?}", vallllue);
                        },
                    }
                }
            }
        }
    }
}

#[allow(unused_variables)]
async fn handle_conn_to_master(stream_to_master: TcpStream, server_info_clone: Arc<Mutex<RedisServer>>,
                               data1: Arc<Mutex<HashMap<String, KeyValueData>>>,
                               exp_heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>) {

    // Local variable of self_port (listening_port)
    let self_port = { server_info_clone.lock().unwrap().self_port.clone() };

    // // Init of the ValuePool
    // let mut value_pool = ValuePool::new();

    let mut handler = RespHandler::new(stream_to_master);

    // Start of the HandShake
    let mut handshake_steps_done = 0;
    println!("Connection to master handled, trying to send HandShake.");
    while handshake_steps_done < 6 {
        let value = if handshake_steps_done == 5 {
            handler.read_hex().await.unwrap()
        } else if handshake_steps_done == 0 {
            Some(Value::SimpleString("Initiator".to_string()))
        } else {
            handler.read_value().await.unwrap()
        };
        handshake_steps_done = handshake_steps(&mut handler, &mut handshake_steps_done, value.unwrap(), self_port, server_info_clone.clone()).await.unwrap();
    }

    // Start the count of offset bytes of propagated commands from master node
    let mut offset = 0;



    loop {
        let value = handler.read_value_and_count().await.unwrap();
        if let Some((v, to_add)) = value {
            println!("Got value from master {:?}", v);
            let (command, args) = extract_command(v).unwrap();
            match command.to_ascii_lowercase().as_str() {
                "set"   => {
                    let data2 = Arc::clone(&data1);
                    let exp_heap2 = Arc::clone(&exp_heap1);
                    data_set(args, data2, exp_heap2);
                },
                "info"  => {
                    server_info(server_info_clone.clone(), args);
                },
                "replconf" => {
                    let res = configure_replica(args, offset);
                    match res {
                        Value::Array(_) => {
                            handler.write_value(res).await.unwrap();
                        },
                        _ => {}
                    }
                },
                "ping" => {},
                c => panic!("Cannot handle command {}", c),
            }
            offset += to_add;
        }
    }
}

async fn handshake_steps(handler_to_master: &mut RespHandler, handshake_steps_done_orig: &mut usize, value: Value, self_port: u16, server_info: Arc<Mutex<RedisServer>>) -> Result<usize>{
    let handshake_steps_done = handshake_steps_done_orig.clone();
    return match handshake_steps_done {
        0       => {
            // HandShake (1/3)
            let handshake = Value::Array(vec![Value::BulkString("PING".to_string())]);
            handler_to_master.write_value(handshake).await?;
            Ok(handshake_steps_done + 1)
        },
        1       => {
            // HandShake (2/3)
            if value == Value::SimpleString("PONG".to_string()) {
                let cmd = vec![
                    Value::BulkString("REPLCONF".to_string()),
                    Value::BulkString("listening-port".to_string()),
                    Value::BulkString(format!("{}", self_port)),
                ];
                let handshake = Value::Array(cmd);
                handler_to_master.write_value(handshake).await?;
                return Ok(handshake_steps_done + 1);
            }
            Err(anyhow::anyhow!(format!("Error during handshake step : {}", handshake_steps_done + 1)))
        },
        2       => {
            if value == Value::SimpleString("OK".to_string()) {
                let cmd = vec![
                    Value::BulkString("REPLCONF".to_string()),
                    Value::BulkString("capa".to_string()),
                    Value::BulkString("psync2".to_string()),
                ];
                let handshake = Value::Array(cmd);
                handler_to_master.write_value(handshake).await?;
                return Ok(handshake_steps_done + 1);
            }
            Err(anyhow::anyhow!(format!("Error during handshake step : {}", handshake_steps_done + 1)))
        },
        3       => {
            // HandShake (2/3)
            if value == Value::SimpleString("OK".to_string()) {
                let cmd = vec![
                    Value::BulkString("PSYNC".to_string()),
                    Value::BulkString("?".to_string()),
                    Value::BulkString("-1".to_string()),
                ];
                let handshake = Value::Array(cmd);
                handler_to_master.write_value(handshake).await?;
                return Ok(handshake_steps_done + 1);
            }
            Err(anyhow::anyhow!(format!("Error during handshake step : {}", handshake_steps_done + 1)))
        },
        4       => {

            let (command, args) = extract_command(value)?;
            match command.to_ascii_lowercase().as_str() {
                "fullresync"      => {
                    if let Value::SimpleString(s) = args[0].clone() {
                        {
                            println!("Changed internal master_id to : {}", s);
                            let _temp = server_info.lock().unwrap().change_replid(s);

                        };
                        return Ok(handshake_steps_done + 1);
                    };
                    return Err(anyhow::anyhow!(format!("Error during handshake step : {}", handshake_steps_done + 1)));
                },
                c     => panic!("Cannot handle command {} during handshake", c),
            }


        },
        5       => {
            // Receiving the RDB file
            // Process the transfer of RDB file ..
            println!("Got the RDB File : {:?}", value);
            Ok(handshake_steps_done + 1)

        }
        _   => {
            return Err(anyhow::anyhow!(format!("Error during handshake step : {}", handshake_steps_done + 1)));
        }
    };
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
        Value::SimpleInteger(_i) => {
            Err(anyhow::anyhow!("SimpleInteger value response is todo!"))
        }
        Value::BulkRawHexFile(_s) => {
            Err(anyhow::anyhow!("BulkStringFile value response is todo!"))
        }
        Value::SimpleCommand(_s) => {
            Err(anyhow::anyhow!("SimpleCommand value response is todo!"))
        }
        Value::SimpleError(_e) => {
            Err(anyhow::anyhow!("SimpleError value response is todo!"))
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

