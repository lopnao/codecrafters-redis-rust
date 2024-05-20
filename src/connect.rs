use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use crate::resp::Value;
use crate::{RedisServer, SlaveError, unpack_bulk_str};
use crate::SlaveError::NoHost;
use base64::prelude::*;

pub async fn connect_to_master(master_host: Option<String>, master_port: Option<u16>) -> Result<TcpStream, SlaveError> {
    if master_host.is_none() { return Err(NoHost); }
    let mut port = 6379;
    if !master_port.is_none() {
        port = master_port.unwrap();
    }
    let stream = TcpStream::connect(format!("{}:{}", master_host.unwrap(), port)).await?;

    Ok(stream)
}


pub fn configure_replica(args: Vec<Value>, offset: usize) -> Value {
    if args.is_empty() { return Value::NullBulkString(); }
    let first_arg = args[0].clone();
    match first_arg {
        Value::BulkString(s) if s.as_str() == "listening-port" => {
            let slave_port = unpack_bulk_str(args[1].clone()).unwrap().parse::<u16>().unwrap();
            println!("Received slave port of : {:?}", slave_port);
            return Value::SimpleString("OK".to_string());
        },
        Value::BulkString(s) if s.as_str() == "capa" => {
            if args[1] == Value::BulkString("psync2".to_string()) {
                return Value::SimpleString("OK".to_string());
            }
            return Value::SimpleString("OK".to_string());
        },
        Value::BulkString(s) if s.to_ascii_lowercase().as_str() == "getack" => {

            if args.len() > 1 {
                if let Value::BulkString(second) = args[1].clone() {
                    match second.to_ascii_lowercase().as_str() {
                        "*" => {
                            let values = vec![
                                Value::BulkString("REPLCONF".to_string()),
                                Value::BulkString("ACK".to_string()),
                                Value::BulkString(format!("{}", offset)),

                            ];
                            return Value::Array(values);
                        },
                        _ => {},
                    }
                }

            }
            Value::NullBulkString()
        },
        v => { v }
    }
}

pub fn psync(args: Vec<Value>, server_info: Arc<Mutex<RedisServer>>) -> Value {
    if  args[0] == Value::BulkString("?".to_string()) &&
        args[1] == Value::BulkString("-1".to_string()) {
        let master_replid = { server_info.lock().unwrap().master_replid.clone() };
        return Value::SimpleString(format!("FULLRESYNC {} 0", master_replid));
    }
    Value::SimpleString("NOK".to_string())
}

pub fn send_rdb_base64_to_hex(files_b64: &str) -> Value {
    let hex_str = BASE64_STANDARD.decode(files_b64).unwrap();

    Value::BulkRawHexFile(hex_str)
}
