use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex};
use std::thread;
use resp::Value;
use tokio::net::{TcpListener, TcpStream};
use anyhow::Result;
use tokio::time::Instant;
use db::KeyValueData;
use crate::db::{data_get, data_set, key_expiry_thread};

mod resp;
mod db;

const EXPIRY_LOOP_TIME: u64 = 1; // 500 milli seconds

#[tokio::main]
async fn main() {
    println!("binding to port : {:?}", 6379);

    let listener = TcpListener::bind("0.0.0.0:6379").await.unwrap();
    let data: Arc<Mutex<HashMap<String, KeyValueData>>> = Arc::new(Mutex::new(HashMap::new()));
    let exp_heap: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>> = Arc::new(Mutex::new(BinaryHeap::new()));

    let data_clean = Arc::clone(&data);
    let exp_clean = Arc::clone(&exp_heap);
    let _ = thread::spawn(move || key_expiry_thread(data_clean, exp_clean, EXPIRY_LOOP_TIME));



    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                let data1 = Arc::clone(&data);
                let exp_heap1 = Arc::clone(&exp_heap);
                tokio::spawn(async move {
                    handle_conn(stream, data1, exp_heap1).await
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }

    }
}

async fn handle_conn(stream: TcpStream, data1: Arc<Mutex<HashMap<String, KeyValueData>>>, exp_heap1: Arc<Mutex<BinaryHeap<(Reverse<Instant>, String)>>>) {
    let mut handler = resp::RespHandler::new(stream);
    println!("Starting read loop");

    loop {
        let value = handler.read_value().await.unwrap();
        println!("Got value {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.to_ascii_lowercase().as_str() {
                "ping"  => Value::SimpleString("PONG".to_string()),
                "echo"  => args.first().unwrap().clone(),
                "set"   => {
                    let data2 = Arc::clone(&data1);
                    let exp_heap2 = Arc::clone(&exp_heap1);
                    data_set(args, data2, exp_heap2)
                },
                "get"   => {
                    let data2 = Arc::clone(&data1);
                    data_get(args, data2)

                },
                c => panic!("Cannot handle command {}", c),
            }
        } else {
            break;
        };
        println!("Sending value {:?}", response);

        handler.write_value(response).await.unwrap();
    }
}


fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::SimpleString(_s) => {
            Err(anyhow::anyhow!("SimpleString value response is todo!"))
        }
        Value::BulkString(_s) => {
            Err(anyhow::anyhow!("BulkString value response is todo!"))
        }
        Value::NullBulkString() => {
            Err(anyhow::anyhow!("NullBulkString value response is todo!"))
        }
        Value::Array(a) => {
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

