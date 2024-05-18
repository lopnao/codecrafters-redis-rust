use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::time::Instant;
use std::time::Duration;
use crate::resp::Value;
use crate::{RedisServer, unpack_bulk_str};

pub fn data_set(args: Vec<Value>, data1: Arc<Mutex<HashMap<String, KeyValueData>>>, heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>) -> Value {
    let mut data1 = data1.lock().unwrap();
    let mut heap1 = heap1.lock().unwrap();
    match args.len() {
        2   => {
            let key = unpack_bulk_str(args[0].clone()).unwrap();
            let value = unpack_bulk_str(args[1].clone()).unwrap();
            data1.insert(key.clone(), KeyValueData::new(key.clone(), value.clone(), 0));
            Value::SimpleString("OK".to_string())
        }
        4 if unpack_bulk_str(args[2].clone()).unwrap().to_ascii_lowercase() == "px".to_string() => {
            let key = unpack_bulk_str(args[0].clone()).unwrap();
            let value = unpack_bulk_str(args[1].clone()).unwrap();
            let expiry = unpack_bulk_str(args[3].clone()).unwrap().parse::<u64>().unwrap();
            let key_data_value = KeyValueData::new(key.clone(), value.clone(), expiry);
            heap1.push((Reverse(key_data_value.expiring_at.clone()), key_data_value.key.clone()));
            data1.insert(key.clone(), key_data_value);

            Value::SimpleString("OK".to_string())
        }
        _ => { Value::SimpleString("NOK".to_string()) }
    }
}

pub fn data_get(args: Vec<Value>, data1: Arc<Mutex<HashMap<String, KeyValueData>>>) -> Value {
    let data1 = data1.lock().unwrap();
    if !args.is_empty() {
        let key_value_data = data1.get(&unpack_bulk_str(args[0].clone()).unwrap());
        match key_value_data {
            Some(key_value_data) => { Value::SimpleString(key_value_data.get_value()) },
            None    => { Value::NullBulkString() }
        }
    } else { Value::SimpleString("ERROR".to_string()) }
}

pub fn server_info(server_info_clone: Arc<Mutex<RedisServer>>, args: Vec<Value>) -> Value {
    let mut res = vec![];
    let args: Vec<String> = args.iter().map(|arg| unpack_bulk_str(arg.clone()).unwrap()).collect();
    for arg in args {
        match arg.as_str() {
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
                let master_nanoid = { server_info_clone.lock().unwrap().master_nanoid.clone() };
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

#[derive(Debug, Clone)]
pub struct KeyValueData {
    key: String,
    value: String,
    _expires: bool,
    _inserted_at: Instant, // time stamp when this key was inserted
    expiring_at: Instant,
}

impl KeyValueData {
    fn new(key: String, value: String, expiry: u64) -> Self {
        let now = Instant::now();
        let expires = match expiry {
            0 => { false },
            _ => { true }
        };
        Self {
            key,
            value,
            _expires: expires,
            _inserted_at: now,
            expiring_at: now + Duration::from_millis(expiry),
        }
    }

    fn get_value(&self) -> String {
        self.value.clone()
    }

    fn _get_expires(&self) -> bool {
        self._expires
    }
}

pub fn key_expiry_thread(data1: Arc<Mutex<HashMap<String, KeyValueData>>>, exp_heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>, loop_every_in_ms: u64) {
    let initial_duration = Duration::from_millis(loop_every_in_ms);
    let mut sleep_duration = Duration::from_millis(loop_every_in_ms);

    loop {
        let now = Instant::now();
        if sleep_duration != initial_duration {
            sleep_duration = initial_duration;
        }


        {
            let mut data2 = data1.lock().unwrap();
            let mut exp_heap2 = exp_heap1.lock().unwrap();

            // go over expiring entries
            while let Some((Reverse(instant), key)) = exp_heap2.peek() {
                if *instant < now {
                    println!("Removing {:?} from data with expiring_time = {:?} // time now is {:?}", key, *instant, now);
                    data2.remove(key);
                    exp_heap2.pop();

                } else {
                    let time_to_sleep = *instant - now;
                    println!("Detected time_to_sleep = {:?}", time_to_sleep.as_millis());
                    if time_to_sleep < sleep_duration {
                        sleep_duration = time_to_sleep;
                    }
                    break;
                }
            }

        }
        println!("sleep_duration = {:?}", sleep_duration.as_millis());
        thread::sleep(sleep_duration);
    }
}
