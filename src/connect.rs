use std::sync::{Arc, Mutex};
use tokio::net::TcpStream;
use crate::resp::Value;
use crate::{RedisServer, SlaveError, unpack_bulk_str};
use crate::SlaveError::NoHost;

pub async fn connect_to_master(master_host: Option<String>, master_port: Option<u16>) -> Result<TcpStream, SlaveError> {
    if master_host.is_none() { return Err(NoHost); }
    let mut port = 6379;
    if !master_port.is_none() {
        port = master_port.unwrap();
    }
    let mut stream = TcpStream::connect(format!("{}:{}", master_host.unwrap(), port)).await?;

    Ok(stream)
}


pub fn configure_replica(args: Vec<Value>) -> Value {
    if args[0] == Value::BulkString("listening-port".to_string()) {
        let slave_port = unpack_bulk_str(args[1].clone()).unwrap().parse::<u16>().unwrap();
        println!("Received slave port of : {:?}", slave_port);

        return Value::SimpleString("OK".to_string());
    } else if   args[0] == Value::BulkString("capa".to_string()) &&
                args[1] == Value::BulkString("psync2".to_string()) {
        return Value::SimpleString("OK".to_string());
    }
    Value::SimpleString("NOK".to_string())
}

pub fn psync(args: Vec<Value>, server_info: Arc<Mutex<RedisServer>>) -> Value {
    if  args[0] == Value::BulkString("?".to_string()) &&
        args[1] == Value::BulkString("-1".to_string()) {
        let master_replid = { server_info.lock().unwrap().master_replid };
        return Value::SimpleString("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string());
        // return Value::SimpleString(format!("FULLRESYNC {} 0", master_replid.));
    }
    Value::SimpleString("NOK".to_string())
}