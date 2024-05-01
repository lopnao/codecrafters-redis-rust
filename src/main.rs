use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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
                    while stream.read(&mut buf).unwrap() > 0 {
                        stream.write(b"+PONG\r\n").unwrap();
                    }
                });

            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}