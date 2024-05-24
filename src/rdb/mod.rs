use std::collections::BTreeMap;
use thiserror::Error;
use tokio::fs::{File, metadata};
use tokio::io::AsyncReadExt;
use crate::rdb::RDBError::ParsingError;
use crate::rdb::StringEncodedValue::{StringEncodedString, StringEncodedI8, ListEncodedString, SortedSetEncodedString, StringEncodedI32, StringEncodedI16};

#[derive(Error, Debug)]
#[derive(PartialEq)]
pub enum RDBError {
    #[error("`{0}`")]
    ParsingError(String),
    // #[error("i/o error")]
    // IORDBError(#[from] io::Error),
}


#[derive(Debug, Clone, PartialEq)]
struct RDBFile {
    name: String,
    fields: Vec<RDBField>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
enum RDBField {
    RDBMagicField(MagicField),
    RDBAuxiliaryField(AuxiliaryField),
    DBSelectorField,
    KeyValueField(Vec<KeyValuePair>),


}

#[derive(Debug, Clone, PartialEq)]
pub struct MagicField {
    version: u8,
}

impl MagicField {
    pub fn new(version: u8) -> Self {
        Self {
            version
        }
    }
}

trait ToHex {
    fn to_hex(&self) -> Vec<u8>;
    fn get_version(&self) -> u8;
}

trait FromHex {
    type Item;

    fn from_hex(bytes_vec: &[u8]) -> Result<Self::Item, RDBError>;
}

impl ToHex for MagicField {
    fn to_hex(&self) -> Vec<u8> {
        let version_str = format!("{:0>4}", self.version);
        let mut version_bytes: Vec<u8> = version_str.bytes().collect();
        let mut prefix = vec![
            82, 69, 68, 73, 83,
        ];
        prefix.append(&mut version_bytes);
        prefix
    }

    fn get_version(&self) -> u8 {
        self.version
    }
}

impl FromHex for MagicField {
    type Item = MagicField;
    fn from_hex(bytes_vec: &[u8]) -> Result<MagicField, RDBError> {
        match bytes_vec.len() {
            9 => {
                let version_bytes = &bytes_vec[5..];
                if let Ok(version_str) = String::from_utf8(version_bytes.to_vec()) {
                    if let Ok(version_u8) = u8::from_str_radix(version_str.as_str(), 10) {
                        return Ok(MagicField::new(version_u8));
                    }
                }
                Err(ParsingError("The parsing of version went wrong !".to_string()))
            },
            _ => Err(ParsingError("The length of the MagicField bytes is not 9 !".to_string()))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuxiliaryField {
    keys: Vec<StringEncodedValue>,
    values: Vec<StringEncodedValue>,
}

impl FromHex for AuxiliaryField {
    type Item = AuxiliaryField;

    fn from_hex(data: &[u8]) -> Result<Self::Item, RDBError> {
        let mut cur_ind = 0;
        let _ = return match data[cur_ind] {
            250 => {
                cur_ind += 1;
                let mut keys = vec![];
                let mut values = vec![];

                let fe_ind = read_until_specific_byte(data, 254)?;
                while cur_ind < fe_ind {
                    // Reading the key
                    let (key_to_add, byte_consumed_by_key) = read_simple_encoded_string(data)?;
                    cur_ind += byte_consumed_by_key;
                    // Adding the key to the vec
                    keys.push(key_to_add);
                    // Reading the value
                    let (value_to_add, byte_consumed_by_value) = read_simple_encoded_string(data)?;
                    cur_ind += byte_consumed_by_value;
                    // Adding the value to the vec
                    values.push(value_to_add);
                }
                if cur_ind == fe_ind {
                    return Ok(AuxiliaryField { keys, values });
                }
                Err(ParsingError("First Byte after Auxiliary Field Parsing is not 0xFE".to_string()))
            },
            _ => { Err(ParsingError("First Byte of Auxiliary Field is not 0xFA".to_string())) }
        };

    }
}



#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum StringEncodedValue {
    StringEncodedString(String),
    StringEncodedI8(i8),
    StringEncodedI16(i16),
    StringEncodedI32(i32),
    LZFStringEncodedString(String),
    ListEncodedString(Vec<StringEncodedValue>),
    SortedSetEncodedString(BTreeMap<StringEncodedValue, StringEncodedValue>),
}

#[derive(Debug, Clone, PartialEq)]
struct KeyValuePair {
    value_type: u8,
    key: StringEncodedValue,
    value: StringEncodedValue,
}

#[allow(unused)]
fn read_simple_encoded_string(data: &[u8]) -> Result<(StringEncodedValue, usize), RDBError> {
    let mut cur_ind = 0;
    let length = match data[cur_ind] {
        0..=63 => {
            cur_ind += 1;
            data[cur_ind - 1] as u32
        },
        i if (64..=127).contains(&i) => {
            cur_ind += 2;
            ((i as u32 - 32) << 8) + data[cur_ind - 1] as u32
        },
        i if (128..=191).contains(&i) => {
            cur_ind += 5;
            ((data[cur_ind - 4] as u32) << (8 * 3)) + ((data[cur_ind - 3] as u32) << (8 * 2)) + ((data[cur_ind - 2] as u32) << (8 * 1)) + ((data[cur_ind - 1] as u32) << (8 * 0))
        },
        i if (192..=255).contains(&i) => {
            cur_ind += 1;
            return match (data[cur_ind - 1] as u32) ^ 192 {
                0 => {
                    cur_ind += 1;
                    let int_value = i8::from_be_bytes([data[cur_ind - 1]]);

                    Ok((StringEncodedI8(int_value), cur_ind))
                },
                1 => {
                    cur_ind += 2;
                    let int_value = i16::from_be_bytes([data[cur_ind - 2], data[cur_ind - 1]]);
                    Ok((StringEncodedI16(int_value), cur_ind))
                },
                2 => {
                    cur_ind += 4;
                    let int_value = i32::from_be_bytes([data[cur_ind - 4], data[cur_ind - 3], data[cur_ind - 2], data[cur_ind - 1]]);
                    Ok((StringEncodedI32(int_value), cur_ind))
                },
                _ => { Err(ParsingError("length parsing error ! Last case: Special format.".to_string())) }
            }
        },
        _ => { return Err(ParsingError("length parsing error !".to_string())); }
    };

    return match String::from_utf8(data[cur_ind..(cur_ind + length as usize)].to_vec()) {
        Ok(s) => { Ok((StringEncodedString(s), length as usize + cur_ind)) }
        Err(e) => { Err(ParsingError(format!("Erreur de parsing de la string : {:?}", e))) }
    };
}

fn read_length(data: &[u8]) -> Result<(usize, i32, usize), RDBError> {
    // Read the length from the buffer and return (length, type of following value, bytes consumed)

    let mut cur_ind = 0;
    return match data[cur_ind] {
        i if (0..=63).contains(&i) => { Ok((i as _, -1, 1)) },
        i if (64..=127).contains(&i) => { Ok((((i as usize - 32) << 8) + data[cur_ind - 1] as usize, -1, 2)) },
        i if (128..=191).contains(&i) => {
            let temp = ((data[cur_ind - 4] as usize) << (8 * 3)) + ((data[cur_ind - 3] as usize) << (8 * 2)) + ((data[cur_ind - 2] as usize) << (8 * 1)) + ((data[cur_ind - 1] as usize) << (8 * 0));
            Ok((temp, -1, 5))
        },
        i if (192..=255).contains(&i) => {
            cur_ind += 1;
            match (data[cur_ind - 1] as u32) ^ 192 {
                0 => { return Ok((1, 0, 1)); }, // Return of 0 for type I8
                1 => { return Ok((2, 1, 1)); }, // Return of 1 for type I16
                2 => { return Ok((4, 2, 1)); }, // Return of 1 for type I32
                3 => { return Ok((1, 3, 1)); }, // Return of 1 for type Compressed String, compressedlen and uncompressedlen follow in order after this one byte
                _ => { Err(ParsingError("length parsing error ! Last case: Special format.".to_string())) }
            }
        },
        _ => { Err(ParsingError("length parsing error !".to_string())) }
    };
}

fn read_until_specific_byte(data: &[u8], byte_to_search: u8) -> Result<usize, RDBError> {
    for i in 0..(data.len() - 1) {
        if data[i + 1] == byte_to_search {
            return Ok(i + 1);
        }
    }
    return Err(ParsingError(format!("The byte : {:x} is not in buffer", byte_to_search)));
}

fn read_value_type(data: &[u8]) -> Result<(StringEncodedValue, usize), RDBError> {
    match data[0] {
        0 => { return Ok((StringEncodedString("".to_string()), 1)); }
        1 => { return Ok((ListEncodedString(vec![]), 1)); }
        2 => { return Ok((SortedSetEncodedString(BTreeMap::new()), 1)); }
        3 => { return Ok((SortedSetEncodedString(BTreeMap::new()), 1)); }
        _ => {}
    }

    Err(ParsingError("value type parsing error !".to_string()))
}

fn read_list_encoded_strings(data: &[u8]) -> Result<(StringEncodedValue, usize), RDBError> {
    if let Ok((size_of_list, _, mut cur_ind)) = read_length(data) {
        let mut res = vec![];
        for _cur_string_to_parse in 0..size_of_list {
            match read_simple_encoded_string(&data[cur_ind..]) {
                Ok((StringEncodedString(string_to_push), bytes_consumed)) => {
                    res.push(StringEncodedString(string_to_push));
                    cur_ind += bytes_consumed;
                },
                _ => { return Err(ParsingError("list parsing error !".to_string())); }
            }
        }
        return Ok((ListEncodedString(res), cur_ind));
    }
    Err(ParsingError("list parsing error !".to_string()))
}

fn read_sorted_set(data: &[u8]) -> Result<(StringEncodedValue, usize), RDBError> {
    if let Ok((size_of_map, _, mut cur_ind)) = read_length(data) {
        let mut map: BTreeMap<StringEncodedValue, StringEncodedValue> = BTreeMap::new();
        let mut cur_key;
        let mut cur_value;
        for _cur_keyvalue_to_parse in 0..size_of_map {
            // Parsing the key
            match read_simple_encoded_string(&data[cur_ind..]) {
                Ok((StringEncodedString(key), bytes_consumed)) => {
                    cur_key = key;
                    cur_ind += bytes_consumed;
                },
                _ => { return Err(ParsingError("list parsing error !".to_string())); }
            }
            // Parsing the value
            match data[cur_ind] {
                253 => { cur_ind += 1; cur_value = "NaN".to_string(); }
                254 => { cur_ind += 1; cur_value = "+inf".to_string(); }
                255 => { cur_ind += 1; cur_value = "-inf".to_string(); }
                _ => {
                    match read_simple_encoded_string(&data[cur_ind..]) {
                        Ok((StringEncodedString(value), bytes_consumed)) => {
                            cur_value = value;
                            cur_ind += bytes_consumed;
                        },
                        _ => { return Err(ParsingError("list parsing error !".to_string())); }
                    }
                }
            }

            // Adding them together in the hashmap
            map.insert(StringEncodedString(cur_key), StringEncodedString(cur_value));
        }
        return Ok((SortedSetEncodedString(map), cur_ind));
    }
    Err(ParsingError("sorted_set parsing error !".to_string()))
}

async fn read_rdb_file(path_to_file: &str) -> Result<(), RDBError> {
    let mut f = File::open(path_to_file).await.unwrap();
    let metadata = metadata(path_to_file).await.unwrap();
    let mut buffer = vec![0; metadata.len() as usize];
    f.read(&mut buffer).await.unwrap();

    let data = buffer.as_slice();
    let mut cur_ind = 0;

    // Read Magic Field
    let _magic_field = MagicField::from_hex(&data[..=9])?;
    cur_ind += 9;

    // Read Auxiliary Field
    let auxiliary_field = AuxiliaryField::from_hex(&data[cur_ind..])?;

    Ok(())




}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magic_field() {
        assert_eq!(MagicField::new(115).to_hex(), vec![82, 69, 68, 73, 83, 48, 49, 49, 53]);
        assert_eq!(MagicField::from_hex(&[82, 69, 68, 73, 83, 48, 49, 49, 53]), Ok(MagicField::new(115)));
        assert_eq!(MagicField::from_hex(&[82, 69, 68, 73, 83, 48, 49, 49, 54]), Ok(MagicField::new(116)));
        assert_eq!(MagicField::from_hex(&[82, 69, 68, 73, 83, 48, 50, 49, 54]), Ok(MagicField::new(216)));
        assert_eq!(MagicField::from_hex(&[82, 69, 68, 73, 83, 48, 48, 49, 49]), Ok(MagicField::new(11)));
        assert_eq!(MagicField::to_hex(&MagicField::new(111)), vec![82, 69, 68, 73, 83, 48, 49, 49, 49]);
        assert_eq!(read_simple_encoded_string(&[192, 127]), Ok((StringEncodedI8(127), 2)));
        assert_eq!(read_simple_encoded_string(&[5, 82, 69, 68, 73, 83]), Ok((StringEncodedString("REDIS".to_string()), 6)));
    }

}
