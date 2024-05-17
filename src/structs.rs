// use std::fmt::{Display, Formatter};
// use crate::structs::ServerRole::Master;

// #[derive(Debug, Copy, Clone)]
// pub enum ServerRole {
//     Master,
//     _Slave
// }
//
// impl Display for ServerRole {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.write_fmt(format_args!(
//             "{}", match self {
//                 ServerRole::Master => { "master" },
//                 ServerRole::_Slave => { "slave" },
//             },
//         ))
//     }
// }
//
// #[derive(Debug, Copy, Clone)]
// pub struct RedisRuntime {
//     pub replication_role: ServerRole,
// }
//
// impl RedisRuntime {
//     pub fn new() -> Self {
//         Self {
//             replication_role: Master,
//         }
//     }
// }