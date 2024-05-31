use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::time::{Instant, timeout, Duration};
use crate::{RedisServer, unpack_bulk_str};
use crate::db::{KeyValueData, StreamDB, StreamValueType};
use crate::rdb::RDBError;
use crate::rdb::RDBError::{NoKeyValueToAdd, StreamEntryError};
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

pub fn cmd_keys(args: Vec<Value>, data1: Arc<Mutex<HashMap<String, KeyValueData>>>) -> Value {
    if args.is_empty() { return Value::NullBulkString(); }
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    let local_data = data1.lock().unwrap();
    match args[0].to_ascii_lowercase().as_str() {
        "*" => {
            let keys_to_res: Vec<String> = local_data.keys().cloned().collect();
            let mut res = vec![];
            for key in keys_to_res {
                res.push(Value::BulkString(key));
            }
            Value::Array(res)
        },
        _   => {
            // We could implement the regex pattern search here
            Value::NullBulkString()
        }
    }
}

pub fn get_type(args: Vec<Value>, data1: Arc<Mutex<HashMap<String, KeyValueData>>>, stream_data: Arc<Mutex<StreamDB>>) -> Value {
    if args.is_empty() { return Value::NullBulkString(); }
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    let local_data = data1.lock().unwrap();
    let local_stream_data = stream_data.lock().unwrap();
    if let Some(arg) = args.first() {
        if let Some(_key_value) = local_data.get(arg) {
                return Value::SimpleString("string".to_string());
        } else if let Some(()) = local_stream_data.get_stream_key(arg) {
            return Value::SimpleString("stream".to_string());
        }
    }

    Value::SimpleString("none".to_string())
}


pub fn cmd_xadd(args: Vec<Value>, stream_db: Arc<Mutex<StreamDB>>) -> Result<Value, RDBError> {
    if args.is_empty() { return Err(StreamEntryError("not one arg after the XADD command.".to_string())); }
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    let stream_key = args.first().unwrap();
    let mut option_id = None;
    let mut i = 1;
    let mut id = args[1].split('-').map(|i_str| i_str.parse::<u64>());
    if let Some(Ok(id_part1)) = id.next() {
        if let Some(Ok(id_part2)) = id.next() {
            option_id = Some((id_part1, Some(id_part2)));
        } else { option_id = Some((id_part1, None)); }
        i = 2;
    }
    let mut key_values = vec![];
    while let Some(key) = args.get(i) {
        if let Some(value) = args.get(i + 1) {
            key_values.push((key.clone(), StreamValueType::String(value.clone())));
        }
        i += 2;
    }
    if !key_values.is_empty() {
        println!("Trying to add to stream key_values = {:?}", key_values);
        {
            let mut stream_db_lock = stream_db.lock().unwrap();
            if stream_db_lock.get_stream_key(stream_key) == None {
                stream_db_lock.add_stream_key(stream_key.clone());
            }
            return match stream_db_lock.add_id(stream_key.clone(), option_id, key_values.clone()) {
                Ok((part1, part2)) => Ok(Value::SimpleString(format!("{}-{}", part1, part2))),
                Err(e) => {
                    match e {
                        RDBError::RequestedIdNotAvailable(_) => {
                            Ok(Value::SimpleError("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string()))
                        },
                        RDBError::Id00Error => {
                            Ok(Value::SimpleError("ERR The ID specified in XADD must be greater than 0-0".to_string()))
                        }
                        _ => Err(RDBError::IdError)
                    }
                }
            }
            // return Ok(Value::SimpleString(format!("{}-{}", generated_id.0, generated_id.1)));
        }

    }
    Err(NoKeyValueToAdd)
}

pub fn cmd_xrange(args: Vec<Value>, stream_db: Arc<Mutex<StreamDB>>) -> Result<Value, RDBError> {
    if args.is_empty() { return Err(StreamEntryError("not one arg after the XRANGE command.".to_string())); }
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    let stream_key = args.first().unwrap();
    let mut start_id = args[1].split('-').map(|i_str| i_str.parse::<u64>());
    let mut starting_range = None;
    if let Some(Ok(id_part1)) = start_id.next() {
        if let Some(Ok(id_part2)) = start_id.next() {
            starting_range = Some((id_part1, Some(id_part2)))
        } else {
            starting_range = Some((id_part1, None))
        }
    }
    let mut ending_range = None;
    if args[2].as_str() != "+" {
        let mut ending_id = args[2].split('-').map(|i_str| i_str.parse::<u64>());

        if let Some(Ok(id_part1)) = ending_id.next() {
            if let Some(Ok(id_part2)) = ending_id.next() {
                ending_range = Some((id_part1, Some(id_part2)))
            } else {
                ending_range = Some((id_part1, None))
            }
        }
    }

    println!("DEBUG XRANGE :: starting_range = {:?} // ending_range = {:?}", starting_range, ending_range);
    let stream_db_lock = stream_db.lock().unwrap();
    stream_db_lock.read_range(stream_key, starting_range, ending_range)
}

pub fn cmd_xread(args: Vec<Value>, stream_db: Arc<Mutex<StreamDB>>) -> Result<Value, RDBError> {
    if args.is_empty() { return Err(StreamEntryError("not one arg after the XREAD command.".to_string())); }
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    let n = args.len();
    let mut streams = vec![];
    let mut i = 0;

    let ending_range = None;
    for arg in &args {
        if arg == &"streams".to_string() {
            continue;
        }
        println!("DEBUG :: arg = {:?}", arg);
        i += 1;
        if arg.contains("-") {
            break;
        } else {
            streams.push(arg);
        }
    }
    let mut ids = vec![];
    for ind in i..n {
        if let Some(start_after_id) = args.get(i) {
            let mut id_parsed = start_after_id.split('-').map(|i_str| i_str.parse::<u64>());
            let mut starting_range = None;
            if let Some(Ok(id_part1)) = id_parsed.next() {
                if let Some(Ok(id_part2)) = id_parsed.next() {
                    starting_range = Some((id_part1, Some(id_part2 + 1)));
                } else {
                    starting_range = Some((id_part1, None));
                }
                ids.push(starting_range)
            }
        }
    }



    let mut res = vec![];
    let stream_db_lock = stream_db.lock().unwrap();
    for (i, &stream_key) in streams.iter().enumerate() {
        let starting_range = ids[i];
        println!("DEBUG XREAD :: starting_range = {:?} // stream_key = {:?}", starting_range, stream_key);
        if let Ok(value) = stream_db_lock.read_range(stream_key, starting_range, ending_range) {
            res.push(Value::Array(vec![Value::BulkString(stream_key.clone()), value]));
        }
    }

    Ok(Value::Array(res))
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
