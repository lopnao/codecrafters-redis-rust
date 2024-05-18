use tokio::{net::TcpStream, io::{AsyncReadExt, AsyncWriteExt}};
use bytes::BytesMut;
use anyhow::Result;


#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    SimpleString(String),
    NullBulkString(),
    BulkString(String),
    BulkRawHexFile(Vec<u8>),
    ArrayBulkString(Vec<Value>),
    Array(Vec<Value>),

}





impl Value {
    pub fn serialize(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::NullBulkString() => "$-1\r\n".to_string(),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.chars().count(), s),
            Value::ArrayBulkString(a) => {
                let a_final = a.iter().fold("".to_string(), |acc, s| format!("{}{}\r\n", acc, match s {
                    Value::SimpleString(s) => { s }
                    Value::NullBulkString() => { "" }
                    Value::BulkString(s) => { s }
                    Value::ArrayBulkString(_) => { "" }
                    Value::BulkRawHexFile(_) => { "" }
                    Value::Array(_) => { "" }
                }));
                format!("${}\r\n{}\r\n", a_final.chars().count(), a_final)
            },
            Value::Array(a) => format!("{}", a.iter().fold(format!("*{}\r\n", a.len()), |acc, s| format!("{}{}", acc, s.clone().serialize()),)),
            Value::BulkRawHexFile(_v)    => "".to_string(),
        }
    }
}


pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (v, _) = parse_message(self.buffer.split())?;

        Ok(Some(v))

    }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        match value {
            Value::BulkRawHexFile(mut v) => {
                let mut enrobage = format!("${}\r\n", v.len()).as_bytes().to_vec();
                enrobage.append(&mut v);
                self.stream.write_all(enrobage.as_slice()).await?;
            }
            value   => {
                self.stream.write_all(value.serialize().as_bytes()).await?;
            }
        }

        Ok(())

    }

}

fn parse_message(buffer: BytesMut) -> Result<(Value, usize)> {

    println!("TEMP buffer = {:?}", buffer);
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        '$' => parse_bulk_string(buffer),
        _ => Err(anyhow::anyhow!("Not a known value type {:?}", buffer)),
    }
}

fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize)> {

    if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();

        return Ok((Value::SimpleString(string), len + 1))
    }

    Err(anyhow::anyhow!("Invalid string {:?}", buffer))

}

fn parse_array(buffer: BytesMut) -> Result<(Value, usize)> {

    let (array_length, mut bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let array_length = parse_int(line)?;

        (array_length, len + 1)

    } else {
        return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
    };

    let mut items = vec![];

    for _ in 0..array_length {
        let (array_item, len) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;
        items.push(array_item);
        bytes_consumed += len;

    }

    println!("items = {:?}", items);

    Ok((Value::ArrayBulkString(items), bytes_consumed))

}

fn parse_bulk_string(buffer: BytesMut) -> Result<(Value, usize)> {

    let (bulk_str_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let bulk_str_len = parse_int(line)?;
        (bulk_str_len, len + 1)

    } else {
        return Err(anyhow::anyhow!("Invalid bulk string format {:?}", buffer));
    };

    let end_of_bulk_str = bytes_consumed + bulk_str_len as usize;
    let total_parsed = end_of_bulk_str + 2;

    Ok((Value::BulkString(String::from_utf8(buffer[bytes_consumed..end_of_bulk_str].to_vec())?), total_parsed))
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }

    None
}

fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize() {
        assert_eq!(Value::SimpleString("PONG".to_string()).serialize(), "+PONG\r\n");
        assert_eq!(Value::BulkString("PONG".to_string()).serialize(), "$4\r\nPONG\r\n");
        assert_eq!(Value::Array(vec![Value::BulkString("PONG".to_string())]).serialize(), "*1\r\n$4\r\nPONG\r\n");
    }
}


//  $<length_of_file>\r\n<contents_of_file>
//  $<length_of_file>\r\n\x55\x44