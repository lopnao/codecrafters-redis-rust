use tokio::net::TcpStream;
use crate::SlaveError;
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