use std::collections::HashMap;

use node_driver::{Body, Maelstrom, Message, NodeMetadata};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Read {},
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {},
    Broadcast {
        message: usize,
    },
    BroadcastOk {},
}

fn main() -> anyhow::Result<()> {
    let (mut node_metadata, mut input, mut output) = Maelstrom::init()?;

    let mut topology: HashMap<String, Vec<String>> = HashMap::new();
    let mut messages = Vec::new();

    // main loop: for each message we receive through the input interface (with a payload of type EchoPayload)
    for msg in input.iter::<EchoPayload>() {
        let msg = msg?;
        match msg.body.payload {
            EchoPayload::Read {} => {
                let msgs = messages.clone();
                output.send_msg(msg.to_response(
                    Some(node_metadata.get_next_msg_id()),
                    EchoPayload::ReadOk { messages: msgs },
                ))?
            }
            EchoPayload::Broadcast { message: x } => {
                messages.push(x);
                output.send_msg(msg.to_response(
                    Some(node_metadata.get_next_msg_id()),
                    EchoPayload::BroadcastOk {},
                ))?
            }
            EchoPayload::Topology { topology: top } => {
                topology = top;
            }
            EchoPayload::ReadOk { messages: _ } => {
                panic!("I don't have that");
            }
            EchoPayload::BroadcastOk {} => {
                panic!("I don't have that");
            }
            EchoPayload::TopologyOk {} => {
                panic!("I don't have that");
            }
        }
    }

    Ok(())
}
