mod resp;

use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use crate::resp::Value;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((mut stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    let mut buf = [0; 512];
                    while stream.read(&mut buf).await.unwrap() > 0 {
                        stream.write(b"+PONG\r\n").await.unwrap();
                    }
                });

            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_conn(stream: TcpStream) {
    let mut handler = resp::RespHandler::new(stream);

    println!("Starting read loop");

    loop {
        let value = handler.read_value().await.unwrap();
        println!("Got value : {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.as_str() {
                "ping" => {
                    Value::SimpleString("PONG".to_string())
                }
                "echo" => {
                    args.first().unwrap().clone()
                }
                c => {
                    panic!("Cannot handle this command : {:?}", c)
                }
            }
        } else {
            break;
        };
        println!("Sendind value: {:?}", response);

        handler.write_value(response).await.unwrap()
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>), anyhow::Error> {
    match value {
        Value::SimpleString(s) => {
            Ok((s, vec![]))
        }
        Value::BulkString(s) => {
            Ok((s, vec![]))
        }
        Value::Array(a) => {
            Ok((unpack_bulk_str(a.first().unwrap().clone())?, a.into_iter().skip(1).collect()))
        }
    }
}

fn unpack_bulk_str(value: Value) -> Result<String, anyhow::Error> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string"))
    }
}