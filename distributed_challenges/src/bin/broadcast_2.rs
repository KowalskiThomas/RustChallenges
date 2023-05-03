use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::Sender,
    thread::sleep,
    time::Duration,
};

use node_driver::{Body, InputInterface, Maelstrom, Message};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
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
    // we will use this message to communicate gossip in-between nodes of the cluster
    Gossip {
        known: HashSet<usize>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Event {
    Eof,
    TimeToGossip,
    MessageReceived(Message<BroadcastPayload>),
}

fn read_stdin(tx: Sender<Event>) -> anyhow::Result<()> {
    let mut input = InputInterface::default();

    // main loop: for each message we receive through the input interface (with a payload of type BroadcastPayload)
    for msg in input.iter::<BroadcastPayload>() {
        let msg = msg?;
        tx.send(Event::MessageReceived(msg)).unwrap();
    }

    tx.send(Event::Eof).unwrap();
    Ok(())
}

fn timer(tx: Sender<Event>) {
    loop {
        _ = tx.send(Event::TimeToGossip);
        sleep(Duration::from_secs(1))
    }
}

fn main() -> anyhow::Result<()> {
    let (mut node_metadata, _, mut output) = Maelstrom::init()?;
    let (tx, rx) = std::sync::mpsc::channel::<Event>();

    let tx_clone = tx.clone();
    std::thread::spawn(move || read_stdin(tx_clone));
    std::thread::spawn(move || timer(tx));

    let mut all_known = HashSet::new();
    let mut my_topology: Option<HashMap<String, Vec<String>>> = None;

    loop {
        if let Ok(ev) = rx.try_recv() {
            let mut to_send: Option<Message<BroadcastPayload>> = None;
            match ev {
                Event::Eof => break,
                Event::TimeToGossip => {
                    eprintln!("Gossiping !");
                    if let Some(top) = &my_topology {
                        let my_neighbours = top.get(&node_metadata.node_id);
                        if let Some(my_neighbours) = my_neighbours {
                            for node in my_neighbours.iter() {
                                eprintln!("Sending to {}", node);
                                output
                                    .send_msg(Message {
                                        src: node_metadata.node_id.clone(),
                                        dst: node.clone(),
                                        body: Body {
                                            msg_id: Some(node_metadata.get_next_msg_id()),
                                            in_reply_to: None,
                                            payload: BroadcastPayload::Gossip {
                                                known: all_known.clone(),
                                            },
                                        },
                                    })
                                    .unwrap()
                            }
                        }
                    }
                }
                Event::MessageReceived(msg) => {
                    match &msg.body.payload {
                        BroadcastPayload::Read {} => {
                            eprintln!("Received request to read");
                            let mut msgs = Vec::new();
                            for x in all_known.iter() {
                                msgs.push(*x)
                            }

                            to_send = Some(
                                msg.to_response(None, BroadcastPayload::ReadOk { messages: msgs }),
                            );
                        }
                        BroadcastPayload::Broadcast { message: x } => {
                            eprintln!("Received broadcast {}", x);
                            all_known.insert(*x);

                            to_send = Some(msg.to_response(None, BroadcastPayload::BroadcastOk {}));
                        }
                        BroadcastPayload::Topology { topology } => {
                            eprintln!("Received topology");
                            my_topology = Some(topology.clone());
                            to_send = Some(msg.to_response(None, BroadcastPayload::TopologyOk {}));
                        }
                        BroadcastPayload::Gossip { known } => {
                            eprintln!("Received gossiping");
                            for x in known.iter() {
                                all_known.insert(*x);
                            }
                        }
                        BroadcastPayload::ReadOk { messages: _ } => {
                            panic!("I don't have that");
                        }
                        BroadcastPayload::BroadcastOk {} => {
                            panic!("I don't have that");
                        }
                        BroadcastPayload::TopologyOk {} => {
                            panic!("I don't have that");
                        }
                    }

                    if let Some(mut msg) = to_send {
                        msg.body.msg_id = Some(node_metadata.get_next_msg_id());
                        output.send_msg(msg).unwrap();
                    }
                }
            }
        }
    }

    Ok(())
}
