use std::cmp::Reverse;
use std::collections::{BinaryHeap, BTreeMap, HashMap};
use std::fmt::{Display, Formatter, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;
use tokio::time::{Instant, Duration};
use crate::db::KeyValueType::StringType;
use crate::rdb::{RDBError, RDBFileStruct};
use crate::rdb::RDBError::{Id00Error, RequestedId, RequestedIdNotAvailable, StreamEntryError};
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
pub enum StreamValueType {
    String(String),
    Integer(i64),
    UInteger(u64),
    Hashmap(BTreeMap<String, StreamValueType>)
}

impl Display for StreamValueType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamValueType::String(s) => { f.write_str(s) }
            StreamValueType::Integer(i) => { f.write_str(i.to_string().as_str()) }
            StreamValueType::UInteger(u) => { f.write_str(u.to_string().as_str()) }
            StreamValueType::Hashmap(m) => { f.write_str("") }
        }
    }
}

pub struct StreamEntity {
    stream_map : BTreeMap<(u64, u64), Vec<(String, StreamValueType)>>,
    last_entry : (u64, u64),
}

impl StreamEntity {
    pub fn init() -> Self {
        Self {
            stream_map: BTreeMap::new(),
            last_entry: (0, 0),
        }
    }

    pub fn check_last_entry(self) -> (u64, u64) {
        self.last_entry
    }

    pub fn change_last_entry(&mut self, id: (u64, u64)) {
        self.last_entry = id;
    }

    pub fn generate_new_id(&self, requested_id : Option<(u64, Option<u64>)>) -> Result<(u64, u64), RDBError> {
        println!("Generate_new_id command with args : requested_id = {:?}", requested_id);
        println!("Current last_entry = {:?} // Current map = {:?}", self.last_entry, self.stream_map);
        let now_id = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
        let mut id_part2 = 0;
        return if let Some((requested_id_part1, opt_requested_id_part2)) = requested_id {
            if let Some(requested_id_part2) = opt_requested_id_part2 {
                if requested_id_part1 == 0 && requested_id_part2 == 0 {
                    return Err(Id00Error)
                }
                if self.last_entry.0 > requested_id_part1 {
                    return Err(RequestedIdNotAvailable((requested_id_part1, requested_id_part2)));
                } else if self.last_entry.0 == requested_id_part1 && self.last_entry.1 >= requested_id_part2 {
                    return Err(RequestedIdNotAvailable((requested_id_part1, requested_id_part2)));
                } else if requested_id_part1 > now_id {
                    return Err(RequestedId((requested_id_part1, requested_id_part2)));
                }
                Ok((requested_id_part1, requested_id_part2))
            } else if self.last_entry.0 == requested_id_part1 {
                Ok((requested_id_part1, self.last_entry.1 + 1))
            } else if self.last_entry.0 < requested_id_part1 {
                Ok((requested_id_part1, id_part2))
            } else {
                Err(RDBError::IdError)
            }

        } else if self.last_entry.0 == now_id {
            id_part2 = self.last_entry.1 + 1;
            Ok((now_id, id_part2))
        } else {
            Ok((now_id, id_part2))
        }
    }
}

pub struct StreamDB {
    streams: BTreeMap<String, StreamEntity>,
}
impl StreamDB {
    pub fn init() -> Self {
        let map = BTreeMap::new();
        Self {
            streams: map
        }
    }

    pub fn add_stream_key(&mut self, key: String) {
        self.streams.insert(key, StreamEntity::init());
    }

    pub fn get_stream_key(&self, key: &str) -> Option<()> {
        if self.streams.contains_key(key) {
            return Some(());
        }
        None
    }

    pub fn add_id(&mut self, key: String, id: Option<(u64, Option<u64>)>, keyvalues: Vec<(String, StreamValueType)>) -> Result<(u64, u64), RDBError> {
        if let Some(mut key_map) = self.streams.get_mut(&key) {
            let id = key_map.generate_new_id(id)?;

            // Insert the key_values and change the last_entry to the current id
            println!("{:?}", id);
            key_map.stream_map.insert(id, keyvalues);
            key_map.change_last_entry(id);

            return Ok(id);
        }
        Err(StreamEntryError("error while adding id to the stream key".to_string()))
    }

    pub fn read_id(&self, key: &str, id: (u64, u64)) -> Result<Vec<(String, StreamValueType)>, RDBError> {

        if let Some(key) = self.streams.get(key) {
            if let Some(id_keyvalues) = key.stream_map.get(&id) {
                return Ok(id_keyvalues.clone())
            }
        }
        Err(StreamEntryError("error while reading id of the stream key".to_string()))
    }

    pub fn read_range(&self, key: &str, id_start: Option<(u64, Option<u64>)>, id_stop: Option<(u64, Option<u64>)>) -> Result<Value, RDBError> {
        let mut ans = vec![];
        if let Some(key) = self.streams.get(key) {
            let mut starting_range = (0, 0);
            let mut ending_range = (0, 0);
            if let Some(id_start) = id_start {
                starting_range.0 = id_start.0;
                if let Some(id_start_end) = id_start.1 {
                    starting_range.1 = id_start_end;
                }
            }

            if let Some(id_stop) = id_stop {
                    ending_range.0 = id_stop.0;
                if let Some(id_stop_end) = id_stop.1 {
                    ending_range.1 = id_stop_end;
                }
            }

            for (stream_id, key_values) in key.stream_map.range(starting_range..=ending_range) {
                let mut vec_id = vec![];
                for (key, value) in key_values {
                    vec_id.push(Value::BulkString(format!("{}", key)));
                    vec_id.push(Value::BulkString(format!("{}", value)));
                }
                ans.push(Value::Array(vec![Value::BulkString(format!("{}-{}", stream_id.0, stream_id.1)), Value::Array(vec_id)]))
            }
            return Ok(Value::Array(ans));
        }
        Err(StreamEntryError("error while reading id of the stream key".to_string()))
    }


}

#[derive(Debug, Clone)]
pub enum KeyValueType {
    StringType,
    NoneType,
    StreamType(BTreeMap<String, BTreeMap<String, Vec<KeyValueData>>>)
}
#[derive(Debug, Clone)]
pub struct KeyValueData {
    key: String,
    value: String,
    expires: bool,
    _inserted_at: Instant, // time stamp when this key was inserted
    expiring_at: Instant,
    keyvalue_type: KeyValueType,
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
            keyvalue_type: StringType,
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
