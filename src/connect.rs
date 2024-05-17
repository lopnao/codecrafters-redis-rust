use tokio::net::TcpStream;
use crate::resp::Value;
use crate::{SlaveError, unpack_bulk_str};
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