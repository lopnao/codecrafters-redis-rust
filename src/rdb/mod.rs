use std::collections::{BTreeMap, HashMap};
use std::num::ParseIntError;
use thiserror::Error;
use tokio::fs::{File, metadata};
use tokio::io::AsyncReadExt;
use crate::db::KeyValueData;
use crate::rdb::RDBError::ParsingError;
use crate::rdb::StringEncodedValue::{StringEncodedString, StringEncodedI8, ListEncodedString, SortedSetEncodedString, StringEncodedI32, StringEncodedI16, NoneValue};

#[allow(dead_code)]
#[derive(Error, Debug)]
#[derive(PartialEq)]
pub enum RDBError {
    #[error("`{0}`")]
    ParsingError(String),
    #[error("`{0}`")]
    StreamEntryError(String),
    #[error("the requested id : `{0:?}` is not available for the stream.")]
    RequestedId((u64, u64)),
    #[error("the requested id : `{0:?}` is not available for the stream.")]
    RequestedIdNotAvailable((u64, u64)),
    #[error("there is a last entry.")]
    InvalidTimeId,
    #[error("general error regarding new id to stream.")]
    IdError,
    #[error("requested id for stream need to be >= 0-1")]
    Id00Error,
    #[error("nothing to add with XADD command, parsing error ?")]
    NoKeyValueToAdd,
    // #[error("i/o error")]
    // IORDBError(#[from] io::Error),
}


#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
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
    RDBKeyValueField(KeyValueField),


}

#[derive(Debug, Clone, PartialEq)]
pub struct RDBFileStruct {
    magic_field: Option<MagicField>,
    auxiliary_field: Option<AuxiliaryField>,
    pub key_value_fields: Vec<Option<KeyValueField>>,
    total_bytes_on_disk: usize,
}

impl RDBFileStruct {
    fn new(magic_field: Option<MagicField>, auxiliary_field: Option<AuxiliaryField>, key_value_fields: Vec<Option<KeyValueField>>, total_bytes_on_disk: usize) -> Self {
        Self {
            magic_field,
            auxiliary_field,
            key_value_fields,
            total_bytes_on_disk
        }
    }

    #[allow(dead_code)]
    pub fn get_map(self) -> Result<HashMap<String, KeyValueData>, RDBError> {

        Err(ParsingError("error while parsing the database from file to the hashmap in memory.".to_string()))
    }
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

    #[allow(dead_code)]
    pub fn get_version(&self) -> u8 {
        self.version
    }
}

#[allow(dead_code)]
trait ToHex {
    fn to_hex(&self) -> Vec<u8>;
}

trait FromHex {
    type Item;

    fn from_hex(bytes_vec: &[u8]) -> Result<(Option<Self::Item>, usize), RDBError>;
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
}

impl FromHex for MagicField {
    type Item = MagicField;
    fn from_hex(data_magic: &[u8]) -> Result<(Option<Self::Item>, usize), RDBError> {
        match data_magic.len() {
            9 => {
                if &data_magic[..5] != [82, 69, 68, 73, 83] {
                    return Err(ParsingError("The magic 'REDIS' is not there !".to_string()))
                }
                let version_bytes = &data_magic[5..];
                if let Ok(version_str) = String::from_utf8(version_bytes.to_vec()) {
                    if let Ok(version_u8) = u8::from_str_radix(version_str.as_str(), 10) {
                        return Ok((Some(MagicField::new(version_u8)), 9));
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

    fn from_hex(data: &[u8]) -> Result<(Option<Self::Item>, usize), RDBError> {
        let mut cur_ind = 0;
        let fe_ind = read_until_specific_byte_is_minimum_of(data, 251)?;
        let mut keys = vec![];
        let mut values = vec![];



        while cur_ind < fe_ind + 1 {
            // Reading the 0xfa signature of Auxiliary Field
            match data[cur_ind] {
                250         => { cur_ind += 1; }
                (251..=255) => { return Ok((Some(AuxiliaryField { keys, values }), cur_ind)); }
                _           => { return Err(ParsingError("First byte of parsing loop is not (0xfa - 0xff)".to_string())); }
            }
            // Reading the key
            let (key_to_add, byte_consumed_by_key) = read_simple_encoded_string(&data[cur_ind..])?;
            cur_ind += byte_consumed_by_key;
            // Adding the key to the vec
            keys.push(key_to_add);
            // Reading the value
            let (value_to_add, byte_consumed_by_value) = read_simple_encoded_string(&data[cur_ind..])?;
            cur_ind += byte_consumed_by_value;
            // Adding the value to the vec
            values.push(value_to_add);

        }
        return Err(ParsingError("First byte after Auxiliary Field parsing is not in (0xfb..0xff) range".to_string()));
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyValueField {
    pub map: BTreeMap<StringEncodedValue, KeyValuePair>
}

impl KeyValueField {
    pub fn get_map(self) -> BTreeMap<StringEncodedValue, KeyValuePair> {
        self.map
    }
}

impl FromHex for KeyValueField {
    type Item = KeyValueField;

    fn from_hex(data: &[u8]) -> Result<(Option<Self::Item>, usize), RDBError> {
        let mut cur_ind = 0;
        let fe_ind = read_until_specific_byte_is_minimum_of(data, 254)?;
        let mut map: BTreeMap<StringEncodedValue, KeyValuePair> = BTreeMap::new();



        while cur_ind < fe_ind + 1 {
            // Init the expiry
            let mut expiry: u8 = 0;
            let mut expiry_time_in_sec: Option<u32> = None;
            let mut expiry_time_in_millisec: Option<u64> = None;

            // Reading the expiry if it exists
            match data[cur_ind] {
                253         => {
                    println!("Expiry buffer : {:?}", data[cur_ind..cur_ind + 5].to_vec());
                    expiry = 1;
                    let temp_buffer = [data[cur_ind + 1], data[cur_ind + 2], data[cur_ind + 3], data[cur_ind + 4]];
                    expiry_time_in_sec = Option::from(u32::from_ne_bytes(temp_buffer));
                    cur_ind += 5;
                },
                252         => {
                    println!("Expiry buffer : {:?}", data[cur_ind..cur_ind + 9].to_vec());
                    
                    expiry = 2;
                    let temp_buffer = [data[cur_ind + 1], data[cur_ind + 2], data[cur_ind + 3],
                        data[cur_ind + 4],data[cur_ind + 5], data[cur_ind + 6], data[cur_ind + 7], data[cur_ind + 8]];
                    expiry_time_in_millisec = Option::from(u64::from_ne_bytes(temp_buffer));
                    cur_ind += 9;
                },
                (254..=255) => { return Ok((Some(KeyValueField { map }), cur_ind)); }
                // No Expiry
                _           => {}
            }
            // Reading the value type
            let value_type_byte = data[cur_ind];
            cur_ind += 1;
            #[allow(unused_assignments)]
            let mut value = NoneValue;

            // Reading the key
            let (key, bytes_consumed_by_value) = read_simple_encoded_string(&data[cur_ind..])?;
            cur_ind += bytes_consumed_by_value;

            // Reading the value
            match value_type_byte {
                0   => {
                    let (value_to_add, bytes_consumed_by_value) = read_simple_encoded_string(&data[cur_ind..])?;
                    value = value_to_add;
                    cur_ind += bytes_consumed_by_value;
                },
                1   => {
                    let (value_to_add, bytes_consumed_by_value) = read_list_encoded_strings(&data[cur_ind..])?;
                    value = value_to_add;
                    cur_ind += bytes_consumed_by_value;
                },
                2   => {
                    let (value_to_add, bytes_consumed_by_value) = read_list_encoded_strings(&data[cur_ind..])?;
                    value = value_to_add;
                    cur_ind += bytes_consumed_by_value;
                },
                3   => {
                    let (value_to_add, bytes_consumed_by_value) = read_sorted_set(&data[cur_ind..])?;
                    value = value_to_add;
                    cur_ind += bytes_consumed_by_value;
                },
                4   => {
                    let (value_to_add, bytes_consumed_by_value) = read_hash_map(&data[cur_ind..])?;
                    value = value_to_add;
                    cur_ind += bytes_consumed_by_value;
                },
                _ => { return Err(ParsingError("Error while parsing key-value pair in KeyValueField parsing from hex".to_string())) },
            }
            // Adding the key_value to the map
            match expiry {
                1   => {
                    let now_in_millis = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis();
                    if let Some(time_in_sec) = expiry_time_in_sec {
                        if time_in_sec * 1000 < now_in_millis as u32 {
                            expiry_time_in_sec = None;
                        } else {
                            let temp = time_in_sec.checked_sub(now_in_millis as u32 / 1000).unwrap();
                            expiry_time_in_sec = Some(temp);
                        }
                    }
                }
                2   => {
                    let now_in_millis = std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_millis();
                    if let Some(time_in_millisec) = expiry_time_in_millisec {
                        println!("timestamp_now_in_millis = {:?} // expiry_in_millis = {:?}", now_in_millis, time_in_millisec);
                        if time_in_millisec <= now_in_millis as u64 {
                            expiry_time_in_millisec = None;
                        } else {
                            let temp = time_in_millisec.checked_sub(now_in_millis as u64).unwrap();
                            expiry_time_in_millisec = Some(temp);
                        }

                    }
                }
                _   => {}
            }
            let key_value_pair_to_add = KeyValuePair::new(key.clone(), value, expiry, expiry_time_in_sec, expiry_time_in_millisec);
            map.insert(key, key_value_pair_to_add);

        }
        return Err(ParsingError("First byte after Auxiliary Field parsing is not in (0xfb..0xff) range".to_string()));
    }
}



#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum StringEncodedValue {
    StringEncodedString(String),
    StringEncodedI8(i8),
    StringEncodedI16(i16),
    StringEncodedI32(i32),
    #[allow(dead_code)]
    LZFStringEncodedString(String),
    ListEncodedString(Vec<StringEncodedValue>),
    SortedSetEncodedString(BTreeMap<StringEncodedValue, StringEncodedValue>),
    NoneValue
}

impl StringEncodedValue {
    pub fn to_string(self) -> String {
        match self {
            StringEncodedString(s) => { s }
            StringEncodedI8(i) => { format!("{i}") }
            StringEncodedI16(i) => { format!("{i}") }
            StringEncodedI32(i) => { format!("{i}") }
            StringEncodedValue::LZFStringEncodedString(s) => { s }
            ListEncodedString(_v) => { "todo!".to_string() }
            SortedSetEncodedString(_map) => { "todo!".to_string() }
            NoneValue => { "".to_string() }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyValuePair {
    key: StringEncodedValue,
    value: StringEncodedValue,
    expiry: u8, // if 0 -> no expiry, if 1 -> expiry is in sec, if 2 -> expiry in msec
    expiry_time_in_sec: Option<u32>,
    expiry_time_in_millisec: Option<u64>,
}

impl KeyValuePair {
    fn new(key: StringEncodedValue, value: StringEncodedValue, expiry: u8, expiry_time_in_sec: Option<u32>, expiry_time_in_millisec: Option<u64>) -> Self {
        Self {
            key,
            value,
            expiry,
            expiry_time_in_sec,
            expiry_time_in_millisec
        }
    }

    pub fn to_data1_map(self) -> Option<KeyValueData> {
        let expiring_at = if self.expiry == 2 {
            if let Some(t_in_millis) = self.expiry_time_in_millisec {
                t_in_millis
            } else { return None; }
        } else if self.expiry == 1 {
            if let Some(t_in_sec) = self.expiry_time_in_sec {
                t_in_sec as u64 * 1000
            } else { return None; }
        } else { 0 };
        let value = self.value.to_string();
        let key = self.key.to_string();
        Some(KeyValueData::new(key, value, expiring_at))
    }
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
                    let int_value = i8::from_ne_bytes([data[cur_ind - 1]]);

                    Ok((StringEncodedI8(int_value), cur_ind))
                },
                1 => {
                    cur_ind += 2;
                    let int_value = i16::from_ne_bytes([data[cur_ind - 2], data[cur_ind - 1]]);
                    Ok((StringEncodedI16(int_value), cur_ind))
                },
                2 => {
                    cur_ind += 4;
                    let int_value = i32::from_ne_bytes([data[cur_ind - 4], data[cur_ind - 3], data[cur_ind - 2], data[cur_ind - 1]]);
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

#[allow(dead_code)]
fn read_until_specific_byte(data: &[u8], byte_to_search: u8) -> Result<usize, RDBError> {
    for i in 0..(data.len() - 1) {
        if data[i + 1] == byte_to_search {
            return Ok(i + 1);
        }
    }
    return Err(ParsingError(format!("The byte : {:x} is not in buffer", byte_to_search)));
}

fn read_until_specific_byte_is_minimum_of(data: &[u8], byte_to_search: u8) -> Result<usize, RDBError> {
    for i in 0..(data.len() - 1) {
        if data[i + 1] >= byte_to_search {
            return Ok(i + 1);
        }
    }
    return Err(ParsingError(format!("The byte : {:x} is not in buffer", byte_to_search)));
}

#[allow(dead_code)]
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

fn read_hash_map(data: &[u8]) -> Result<(StringEncodedValue, usize), RDBError> {
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
                _ => { return Err(ParsingError("hash_map parsing error ! (key)".to_string())); }
            }
            // Parsing the value
            match read_simple_encoded_string(&data[cur_ind..]) {
                Ok((StringEncodedString(value), bytes_consumed)) => {
                    cur_value = value;
                    cur_ind += bytes_consumed;
                },
                _ => { return Err(ParsingError("hash_map parsing error ! (value)".to_string())); }
            }



            // Adding them together in the hashmap
            map.insert(StringEncodedString(cur_key), StringEncodedString(cur_value));
        }
        return Ok((SortedSetEncodedString(map), cur_ind));
    }
    Err(ParsingError("hash_map parsing error ! (size)".to_string()))
}



pub async fn read_rdb_file(path_to_file: &str) -> Result<(RDBFileStruct, usize), RDBError> {
    let mut f = File::open(path_to_file).await.unwrap();
    let metadata = metadata(path_to_file).await.unwrap();
    let mut buffer = vec![0; metadata.len() as usize];
    f.read(&mut buffer).await.unwrap();

    let data = buffer.as_slice();
    let mut cur_ind = 0;

    println!("[DEBUG] :::::::::: RDBFile ReadBuffer ::::::::::");
    println!("{:x?}", data);

    #[allow(unused_assignments)]
    let mut magic_field = None;
    // Read Magic Field
    let (magic_field_to_add, bytes_consumed) = MagicField::from_hex(&data[..9])?;
    magic_field = magic_field_to_add;
    cur_ind += bytes_consumed;
    println!("Magic Field has been read: {:?}", magic_field);

    #[allow(unused_assignments)]
    let mut auxiliary_field = None;
    // Read Auxiliary Field if present
    let (auxiliary_field_if_present, bytes_consumed) = if data[cur_ind] == 250 { AuxiliaryField::from_hex(&data[cur_ind..])? } else { (None, 0) };
    auxiliary_field = auxiliary_field_if_present;
    cur_ind += bytes_consumed;
    if !auxiliary_field.is_none() {
        println!("Auxiliary Field has been read: {:?}", auxiliary_field);
    }

    // Read the Database number if present
    #[allow(unused_variables, unused_assignments)]
    let mut database_number = 0;
    #[allow(unused_assignments)]
    if data[cur_ind] == 254 {
        database_number = data[cur_ind + 1];
        cur_ind += 2;
    }

    // Read resizedb if present
    let mut hash_table_size: i32 = 0;
    let mut expiry_table_size: i32 = 0;
    if data[cur_ind] == 251 {
        cur_ind += 1;

        println!("There is a resize_db, cur_ind = {:?}", cur_ind);
        // Read the hash_table_size
        let (first_value, int_type, _) = read_length(&data[cur_ind..])?;
        cur_ind += 1;
        match int_type {
            -1  => { hash_table_size = first_value as _ }
            0 => {
                cur_ind += 1;
                hash_table_size = i8::from_ne_bytes([data[cur_ind - 1]]) as _;
            },
            1 => {
                cur_ind += 2;
                hash_table_size = i16::from_ne_bytes([data[cur_ind - 2], data[cur_ind - 1]]) as _;
            },
            2 => {
                cur_ind += 4;
                hash_table_size = i32::from_ne_bytes([data[cur_ind - 4], data[cur_ind - 3], data[cur_ind - 2], data[cur_ind - 1]]) as _;
            },
            _ => {}
        }
        println!("hash_table_size = {:?}", hash_table_size);

        // Read the expiry_table_size
        let (first_value, int_type, _) = read_length(&data[cur_ind..])?;
        cur_ind += 1;
        match int_type {
            -1  => { expiry_table_size = first_value as _ }
            0 => {
                cur_ind += 1;
                expiry_table_size = i8::from_ne_bytes([data[cur_ind - 1]]) as _;
            },
            1 => {
                cur_ind += 2;
                expiry_table_size = i16::from_ne_bytes([data[cur_ind - 2], data[cur_ind - 1]]) as _;
            },
            2 => {
                cur_ind += 4;
                expiry_table_size = i32::from_ne_bytes([data[cur_ind - 4], data[cur_ind - 3], data[cur_ind - 2], data[cur_ind - 1]]) as _;
            },
            _ => {}
        }
        println!("expiry_table_size = {:?}", expiry_table_size);
    }

    println!("cur_ind = {:?}", cur_ind);
    // Read the key_value_pairs if present
    let mut key_value_field = None;
    if (data[cur_ind] != 254) & (data[cur_ind] != 255) {
        println!("Trying to read the key_value fields of RDB File ! cur_ind = {:?}", cur_ind);
        let (key_value_field_if_present, bytes_consumed) = KeyValueField::from_hex(&data[cur_ind..])?;
        key_value_field = key_value_field_if_present;
        cur_ind += bytes_consumed;
    }

    //todo: faire le checksum

    Ok((RDBFileStruct::new(magic_field, auxiliary_field, vec![key_value_field], cur_ind), cur_ind))
}


#[allow(dead_code)]
fn read_hex_from_string(s: &str) -> Result<Vec<u8>, ParseIntError> {
    let mut ans = vec![];
    // let s = "fa 08 75 73  65 64 2d 6d 65 6d c2 b0 c4 10 00";
    let new_s = s.split_whitespace();
    for hex in new_s {
        let dec = u8::from_str_radix(hex, 16)?;
        ans.push(dec);
    }

    Ok(ans)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magic_field() {
        assert_eq!(MagicField::new(115).to_hex(), vec![82, 69, 68, 73, 83, 48, 49, 49, 53]);
        assert_eq!(MagicField::from_hex(&[82, 69, 68, 73, 83, 48, 49, 49, 53]), Ok((Some(MagicField::new(115)), 9)));
        assert_eq!(MagicField::from_hex(&[82, 69, 68, 73, 83, 48, 49, 49, 54]), Ok((Some(MagicField::new(116)), 9)));
        assert_eq!(MagicField::from_hex(&[82, 69, 68, 73, 83, 48, 50, 49, 54]), Ok((Some(MagicField::new(216)), 9)));
        assert_eq!(MagicField::from_hex(&[82, 69, 68, 73, 83, 48, 48, 49, 49]), Ok((Some(MagicField::new(11)), 9)));

        assert_eq!(MagicField::to_hex(&MagicField::new(111)), vec![82, 69, 68, 73, 83, 48, 49, 49, 49]);

        assert_eq!(read_simple_encoded_string(&[192, 127]), Ok((StringEncodedI8(127), 2)));

        assert_eq!(read_simple_encoded_string(&[5, 82, 69, 68, 73, 83]), Ok((StringEncodedString("REDIS".to_string()), 6)));

        assert_eq!(read_hex_from_string("fa 08 75 73  65 64 2d 6d 65 6d c2 b0 c4 10 00"),
                   Ok(vec![250, 8, 117, 115, 101, 100, 45, 109, 101, 109, 194, 176, 196, 16, 0]));

        assert_eq!(AuxiliaryField::from_hex(&[250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, 250, 10, 114, 101,
            100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250, 5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100, 45,
            109, 101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192, 0, 255]),
                   Ok((Some(AuxiliaryField { keys: vec![StringEncodedString("redis-ver".to_string()), StringEncodedString("redis-bits".to_string()),
                                                        StringEncodedString("ctime".to_string()), StringEncodedString("used-mem".to_string()),
                                                        StringEncodedString("aof-base".to_string())],
                                            values: vec![StringEncodedString("7.2.0".to_string()), StringEncodedI8(64), StringEncodedI32(1706821741),
                                                        StringEncodedI32(1098928), StringEncodedI8(0)] }), 70)));
    }

    // fn test_index() {
    //     let data = [52, 45, 44, 49, 53, 30, 30, 30, 33, fa, 9, 72, 65, 64, 69, 73, 2d, 76, 65, 72, 5, 37, 2e, 32, 2e, 30, fa, a, 72, 65, 64, 69, 73, 2d, 62, 69, 74, 73, c0, 40, fe, 0, fb, 1, 0, 0, 5, 67, 72, 61, 70, 65, 9, 72, 61, 73, 70, 62, 65, 72, 72, 79, ff, d5, 39, b9, 58, 15, c1, f3, c0, a]
    // }
    #[test]
    fn test_parse_hex_data() {
        println!("{:?}", read_hex_from_string("52 45 44 49 53 30 30 30 33 fa 09 72 65 64 69 73 2d 76 65 72 05 37 2e 32 2e 30 fa 0a 72 65 64 69 73 2d 62 69 74 73 c0 40 fe 00 fb 01 00 00 05 67 72 61 70 65 09 72 61 73 70 62 65 72 72 79 ff d5 39 b9 58 15 c1 f3 c0 0a"));
    }

    #[test]
    fn read_byte() {
        let cur_ind = 45;
        let prev = cur_ind - 2;
        let next = cur_ind + 2;
        let data = [82, 69, 68, 73, 83, 48, 48, 48, 51, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 254, 0, 251, 1, 0, 0, 5, 103, 114, 97, 112, 101, 9, 114, 97, 115, 112, 98, 101, 114, 114, 121, 255, 213, 57, 185, 88, 21, 193, 243, 192, 10];
        println!("data[{:?}] = {:?}", cur_ind, data[cur_ind]);
        println!("data[{:?}-2..={:?}+2] = {:?}", cur_ind, cur_ind, data[prev..=next].to_vec());

    }

}
