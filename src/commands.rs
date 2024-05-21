use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
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

pub fn wait_or_replicas(args: Vec<Value>, watch_replicas_count_rx: watch::Receiver<usize>) -> Value {
    println!("ON EST ICI : args = {:?}", args);
    let default_time_to_sleep = Duration::from_millis(50);
    let mut time_to_sleep = default_time_to_sleep;
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    println!("ON EST ICI : args = {:?}", args);
    if let Some(number_of_replicas_str) = args.get(0) {
        if let Ok(number_of_replicas) = number_of_replicas_str.parse::<usize>() {
            if let Some(timeout_time_str) = args.get(1) {
                if let Ok(timeout_time_milli) = timeout_time_str.parse::<u64>() {

                    let timeout_time = Instant::now().checked_add(Duration::from_millis(timeout_time_milli)).unwrap();
                    let mut receiver_count = watch_replicas_count_rx.borrow().clone();
                    println!("ICI ON VA ATTENDRE {:?} // ON A {:?} GOOD BOTS", timeout_time_milli, receiver_count);
                    println!("NOW = {:?} et TIMEOUT = {:?}", Instant::now(), timeout_time);
                    while number_of_replicas > receiver_count {
                        let now = Instant::now();
                        if now > timeout_time {
                            receiver_count = watch_replicas_count_rx.borrow().clone();
                            break;
                        }

                        let duration_to_timeout = timeout_time - now;
                        if duration_to_timeout < default_time_to_sleep {
                            time_to_sleep = duration_to_timeout;
                        }

                        sleep(time_to_sleep);
                        receiver_count = watch_replicas_count_rx.borrow().clone();
                    }
                    println!("On envoie la valeur = {:?} à {:?}", receiver_count, Instant::now());
                    return Value::SimpleInteger(receiver_count as i64);
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
