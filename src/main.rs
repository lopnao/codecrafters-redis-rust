use resp::Value;
use tokio::net::{TcpListener, TcpStream};
use anyhow::Result;

mod resp;

#[tokio::main]
async fn main() {
    println!("binding to port : {:?}", 6379);

    let listener = TcpListener::bind("0.0.0.0:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    handle_conn(stream).await
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
        println!("Got value {:?}", value);

        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            match command.to_ascii_lowercase().as_str() {
                "ping"  => Value::SimpleString("PONG".to_string()),
                "echo"  => args.first().unwrap().clone(),
                "set"   => {
                    //todo
                    Value::SimpleString("OK".to_string())
                }
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