use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::time::{Instant, timeout, Duration};
use crate::{RedisServer, unpack_bulk_str};
use crate::resp::Value;



pub fn server_info(server_info_clone: Arc<Mutex<RedisServer>>, args: Vec<Value>) -> Value {
    let mut res = vec![];
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    for arg in args {
        match arg.to_ascii_lowercase().as_str() {
            "replication" => {
                let is_master = { server_info_clone.lock().unwrap().is_master };
                let role = if is_master { "master" } else { "slave" };
                res.push(Value::BulkString("# Replication".to_string()));
                res.push(Value::BulkString(format!("role:{}", role)));
                if is_master {
                    let connected_slaves = { server_info_clone.lock().unwrap().connected_slaves };
                    res.push(Value::BulkString(format!("connected_slaves:{}", connected_slaves)));
                }
                let master_replid = { server_info_clone.lock().unwrap().master_replid.clone() };
                let master_repl_offset = { server_info_clone.lock().unwrap().master_repl_offset };
                let master_nanoid = { server_info_clone.lock().unwrap().self_nanoid.clone() };
                res.push(Value::BulkString(format!("master_replid:{}", master_replid)));
                res.push(Value::BulkString(format!("master_repl_offset:{}", master_repl_offset)));
                res.push(Value::BulkString(format!("master_nanoid:{}", master_nanoid)));
                if !is_master {
                    let master_host = { server_info_clone.lock().unwrap().master_host.clone() };
                    let master_port = { server_info_clone.lock().unwrap().master_port.clone() };
                    if let (Some(master_host), Some(master_port)) = (master_host, master_port) {
                        res.push(Value::BulkString(format!("master_host:{}", master_host)));
                        res.push(Value::BulkString(format!("master_port:{}", master_port)));
                    }
                }
            },
            _ => {
                return Value::NullBulkString();
            },
        }
    }

    Value::ArrayBulkString(res)
}

pub fn server_config(args: Vec<Value>, server_info: Arc<Mutex<RedisServer>>) -> Value {
    let mut res = vec![];
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    let config_cmd = args.get(0);
    match config_cmd {
        None => { Value::NullBulkString() }
        Some(s) if s.to_ascii_lowercase().as_str() == "get" => {
            for arg in args.into_iter().skip(1) {
                match arg.to_ascii_lowercase().as_str() {
                    "dir" => {
                        let dir = {
                            server_info.lock().unwrap().dir.clone()
                        };
                        match dir {
                            None => {},
                            Some(s) => {
                                res.push(Value::BulkString("dir".to_string()));
                                res.push(Value::BulkString(s));
                            },
                        }
                    },
                    "dbfilename" => {
                        let dbfilename = {
                            server_info.lock().unwrap().dbfilename.clone()
                        };
                        match dbfilename {
                            None => {},
                            Some(s) => {
                                res.push(Value::BulkString("dbfilename".to_string()));
                                res.push(Value::BulkString(s));
                            },
                        }
                    },
                    _ => { return Value::NullBulkString(); }
                }
            }
            Value::Array(res)
        },
        _ => { Value::NullBulkString() },
    }
}


pub async fn wait_or_replicas(args: Vec<Value>, mut watch_replicas_count_rx: watch::Receiver<usize>) -> Value {
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    let now_instant = Instant::now();
    if let Some(number_of_replicas_str) = args.get(0) {
        if let Ok(number_of_replicas) = number_of_replicas_str.parse::<usize>() {
            if let Some(timeout_time_str) = args.get(1) {
                if let Ok(timeout_time_milli) = timeout_time_str.parse::<u64>() {
                    let mut replica_count;
                    let timeout_duration = Duration::from_millis(timeout_time_milli);
                    let timeout_instant = now_instant.clone() + timeout_duration;
                    {
                        replica_count = watch_replicas_count_rx.borrow().clone();
                    }
                    while Instant::now() < timeout_instant && replica_count < number_of_replicas {
                        let new_duration_to_timeout_time = timeout_instant - Instant::now();
                        if let Ok(_) = timeout(new_duration_to_timeout_time, watch_replicas_count_rx.clone().changed()).await {
                            {
                                replica_count = *watch_replicas_count_rx.borrow_and_update();
                            }
                        }
                    }
                    //println!("ICI On envoie la valeur = {:?} à {:?}", replica_count, Instant::now());
                    println!("On a mis Duration = {:?}", now_instant - Instant::now());
                    return Value::SimpleInteger(replica_count as i64);
                }
            }
        }
    }
    Value::NullBulkString()
}



#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ChannelCommand {
    command: CCommand,
    args: Option<Value>,

}

#[allow(dead_code)]
impl ChannelCommand {
    pub fn new(command: CCommand, args: Option<Value>) -> Self {
        Self {
            command,
            args
        }
    }
}

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub enum CCommand {
    AddReplica,
    DelReplica,
    Ack,
    Info,

}
