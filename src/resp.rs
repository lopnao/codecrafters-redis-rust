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

#[derive(Clone, Debug, PartialEq)]
pub struct ValuePool {
    pool: Vec<Value>,
    remaining: usize,
}

#[allow(dead_code)]
impl ValuePool {
    pub fn new() -> ValuePool {
        ValuePool {
            pool: vec![],
            remaining: 0,
        }
    }

    async fn next_read_value(&mut self, handler: &mut RespHandler) -> Option<<ValuePool as IntoIterator>::Item> {
        if self.pool.len() == 0 {
            return handler.read_value().await.ok()?
        }
        let result = self.pool.remove(0);
        self.remaining -= 1;
        Some(result)
    }

    async fn next_read_hex(&mut self, handler: &mut RespHandler) -> Option<<ValuePool as IntoIterator>::Item> {
        if self.pool.len() == 0 {
            return handler.read_hex().await.ok()?
        }
        match self.pool[0] {
            Value::BulkRawHexFile(_) => {
                let result = self.pool.remove(0);
                self.remaining -= 1;
                Some(result)
            }
            _       => {
                None
            }
        }

    }

    pub fn add_value(mut self, value: Value) {
        self.pool.push(value);
        self.remaining += 1;
    }

    pub fn pop_value(mut self) -> Option<Value> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        self.pool.pop()

    }
}

pub struct ValuePoolIntoIterator {
    value_pool: ValuePool,
}

impl IntoIterator for ValuePool {
    type Item = Value;
    type IntoIter = ValuePoolIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter { value_pool: self }
    }
}


impl Iterator for ValuePoolIntoIterator {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.value_pool.pool.len() == 0 {
            return None;
        }
        let result = self.value_pool.pool.remove(0);
        self.value_pool.remaining -= 1;
        Some(result)
    }
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
                    Value::BulkString(s) => { s }
                    _ => { "" }
                }));
                format!("${}\r\n{}\r\n", a_final.chars().count(), a_final)
            },
            Value::Array(a) => format!("{}", a.iter().fold(format!("*{}\r\n", a.len()), |acc, s| format!("{}{}", acc, s.clone().serialize()),)),
            Value::BulkRawHexFile(_v)    => "".to_string(),
            // _ => "".to_string(),
        }
    }

    pub fn deserialize_bulkstring(self) -> Value {
        match self {
            Value::BulkString(s) => {
                let s = s.as_bytes();
                let mut vec_value = vec![];
                let mut ind = 0;
                while let Some((a, i)) = read_until_dollar_or_end_with_ind(s, ind) {
                    ind = i;
                    if let Ok((d, _)) = parse_bulk_string(BytesMut::from(a)) {
                        vec_value.push(d);
                    }
                }

                Value::Array(vec_value)
            },
            Value::ArrayBulkString(v) => Value::Array(v),
            v           => {
                println!("ICI v est: {:?}", v);
                panic!("Need to be a BulkString to call unserialize_bulkstring !")
            },
        }
    }

}

#[derive(Debug)]
pub struct RespHandler {
    stream: TcpStream,
    pub buffer: BytesMut,
    value_pool: ValuePool,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler {
            stream,
            buffer: BytesMut::with_capacity(512),
            value_pool: ValuePool::new(),
        }
    }

    pub fn client_addr(&self) -> String {
        return format!("{:?}",self.stream.peer_addr())
    }

    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        if !self.buffer.is_empty() {
            let (v, bytes_consumed) = parse_message(self.buffer.clone())?;
            let _ = self.buffer.split_to(bytes_consumed);
            return Ok(Some(v));
        }
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (v, bytes_consumed) = parse_message(self.buffer.clone())?;
        let _ = self.buffer.split_to(bytes_consumed);
        Ok(Some(v))

    }

    pub async fn read_hex(&mut self) -> Result<Option<Value>> {
        if !self.buffer.is_empty() {
            let (v, bytes_consumed) = parse_hexdump(self.buffer.clone())?;
            let _ = self.buffer.split_to(bytes_consumed);
            return Ok(Some(v));
        }
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        // Added to process RDB File HexDump Transfer
        let (v, _) = parse_hexdump(self.buffer.split())?;

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

fn parse_hexdump(buffer: BytesMut) -> Result<(Value, usize)> {

    println!("TEMP buffer = {:?}", buffer);
    match buffer[0] as char {
        '$' => parse_bulk_hex(buffer),
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


// Voici ce qu on doit parse ::
// b"$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08
// used-mem\xc2\xb0\xc4\x10\0\xfa\x08aof-base\xc0\0\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n
// *3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
fn parse_bulk_hex(buffer: BytesMut) -> Result<(Value, usize)> {
    let (bulk_hex_len, bytes_consumed) = if let Some((line, len)) = read_until_crlf(&buffer[1..]) {
        let bulk_hex_len = parse_int(line)?;
        (bulk_hex_len, len + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid bulk hex format {:?}", buffer));
    };
    let end_of_bulk_hex = bytes_consumed + bulk_hex_len as usize;
    let total_parsed = end_of_bulk_hex + 2;
    println!("In parse_bulk_hex :: BUFFER = {:?} et total_parsed = {:?}", buffer, total_parsed);

    Ok((Value::BulkRawHexFile(buffer[bytes_consumed..end_of_bulk_hex].to_vec()), total_parsed))
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }

    None
}

fn read_until_dollar_or_end_with_ind(buffer: &[u8], ind: usize) -> Option<(&[u8], usize)> {
    for i in ind..(buffer.len() - 1) {
        if buffer[i + 1] == b'$' {
            return Some((&buffer[ind..=i], i + 1));
        }
    }
    if ind < buffer.len() - 2 {
        return Some((&buffer[ind..], buffer.len()));
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

    #[test]
    fn test_parse_bulkstring() {
        assert_eq!(Value::BulkString("$3\r\nSET\r\n$5\r\napple\r\n$4\r\ntree\r\n$2\r\npx\r\n$4\r\n1000\r\n".to_string()).deserialize_bulkstring(),
                   Value::Array(vec![Value::BulkString("SET".to_string()),
                       Value::BulkString("apple".to_string()), Value::BulkString("tree".to_string()),
                       Value::BulkString("px".to_string()), Value::BulkString("1000".to_string())]));
        assert_eq!(Value::BulkString("$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".to_string()).deserialize_bulkstring(),
                   Value::Array(vec![Value::BulkString("SET".to_string()),
                                     Value::BulkString("foo".to_string()),
                                     Value::BulkString("bar".to_string())]));
        assert_eq!(Value::Array(vec![Value::BulkString("SET".to_string()),
                                               Value::BulkString("foo".to_string()),
                                               Value::BulkString("bar".to_string())]).serialize(),
                   "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".to_string());
    }
}


//  $<length_of_file>\r\n<contents_of_file>
//  $<length_of_file>\r\n\x55\x44