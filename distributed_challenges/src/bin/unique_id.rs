use node_driver::{Body, Maelstrom, Message, NodeMetadata};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Generate {},
    GenerateOk { id: String },
}

fn main() -> anyhow::Result<()> {
    let (mut node_metadata, mut input, mut output) = Maelstrom::init()?;

    // main loop: for each message we receive through the input interface (with a payload of type EchoPayload)
    for msg in input.iter::<EchoPayload>() {
        let msg = msg?;
        match msg.body.payload {
            EchoPayload::Generate {} => {
                let id = Uuid::new_v4();
                output.send_msg(msg.to_response(
                    Some(node_metadata.get_next_msg_id()),
                    EchoPayload::GenerateOk { id: id.to_string() },
                ))?
            }
            EchoPayload::GenerateOk { id: _ } => {
                panic!("I don't have that");
            }
        }
    }

    Ok(())
}
