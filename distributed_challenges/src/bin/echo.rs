use node_driver::{Body, Maelstrom, Message};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },
    EchoOk { echo: String },
}

fn main() -> anyhow::Result<()> {
    // init our node by getting its metadata and an output and input interface to communicate
    let (mut node_metadata, mut input, mut output) = Maelstrom::init()?;

    // main loop: for each message we receive through the input interface (with a payload of type EchoPayload)
    for msg in input.iter::<EchoPayload>() {
        let msg = msg?;
        match msg.body.payload {
            EchoPayload::Echo { echo } => output.send_msg(Message {
                src: node_metadata.node_id.clone(),
                dst: msg.src,
                body: Body {
                    msg_id: Some(node_metadata.get_next_msg_id()),
                    in_reply_to: msg.body.msg_id,
                    payload: EchoPayload::EchoOk { echo },
                },
            })?,
            EchoPayload::EchoOk { echo: _ } => {
                panic!("Didn't expect that type");
            }
        }
    }

    Ok(())
}
