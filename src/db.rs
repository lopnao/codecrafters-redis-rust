use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::time::{Instant, Duration};
use crate::rdb::RDBFileStruct;
use crate::resp::Value;
use crate::unpack_bulk_str;

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

pub fn data_set_from_rdb(rdbfile_struct: RDBFileStruct, data1: Arc<Mutex<HashMap<String, KeyValueData>>>, heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>) {
    let mut data1 = data1.lock().unwrap();
    let mut heap1 = heap1.lock().unwrap();

    for option_map in rdbfile_struct.key_value_fields {
        if let Some(inner_map) = option_map {
            let map = inner_map.get_map();
            for key in map.keys().cloned() {
                if let Some(value) = map.get(&key) {
                    let key_value_data = value.clone().to_data1_map();
                    if let Some(key_value_to_insert) = key_value_data {
                        println!("Inserting : {:?} : {:?}", key.clone().to_string(), key_value_to_insert);
                        if key_value_to_insert.expires {
                            heap1.push((Reverse(key_value_to_insert.expiring_at.clone()), key_value_to_insert.key.clone()));
                        }
                        data1.insert(key.to_string(), key_value_to_insert);
                    } else {
                        println!("Not inserting a Key_Value_Data because expiry_time is in the past !")
                    }

                }
            }
        }
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

#[derive(Debug, Clone)]
pub struct KeyValueData {
    key: String,
    value: String,
    expires: bool,
    _inserted_at: Instant, // time stamp when this key was inserted
    expiring_at: Instant,
}

impl KeyValueData {
    pub fn new(key: String, value: String, expiry: u64) -> Self {
        let now = Instant::now();
        let expires = match expiry {
            0 => { false },
            _ => { true }
        };
        Self {
            key,
            value,
            expires,
            _inserted_at: now,
            expiring_at: now + Duration::from_millis(expiry),
        }
    }

    pub fn get_value(&self) -> String {
        self.value.clone()
    }

    fn _get_expires(&self) -> bool {
        self.expires
    }
}

pub fn key_expiry_thread(data1: Arc<Mutex<HashMap<String, KeyValueData>>>,
                         exp_heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>,
                         loop_every_in_ms: u64) {
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
                    if time_to_sleep < sleep_duration {
                        sleep_duration = time_to_sleep;
                    }
                    break;
                }
            }

        }
        thread::sleep(sleep_duration);
    }
}
