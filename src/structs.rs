use crate::resp::Value;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct ChannelCommand {
    command: CCommand,
    args: Option<Value>,

}

#[allow(dead_code)]
impl ChannelCommand {
    pub fn new(command: CCommand, args: Option<Value>) -> Self {
        Self {
            command,
            args
        }
    }
}

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub enum CCommand {
    AddReplica,
    DelReplica,
    Ack,
    Info,

}