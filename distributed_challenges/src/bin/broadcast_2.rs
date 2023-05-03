use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::{Receiver, Sender},
    thread::sleep,
    time::Duration,
};

use node_driver::{Body, InputInterface, Maelstrom, Message, NodeMetadata, OutputInterface};
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

struct Node {
    all_known: HashSet<usize>,
    topology: Option<HashMap<String, Vec<String>>>,

    rx: Option<Receiver<Event>>,
    node_metadata: Option<NodeMetadata>,
    output: Option<OutputInterface>,
}

impl Node {
    fn new() -> Node {
        Node {
            all_known: HashSet::new(),
            topology: None,

            rx: None,
            node_metadata: None,
            output: None,
        }
    }

    fn do_gossiping(&mut self) {
        let node_metadata = self.node_metadata.as_mut().unwrap();
        let output = self.output.as_mut().unwrap();

        eprintln!("Gossiping !");
        if let Some(top) = &self.topology {
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
                                    known: self.all_known.clone(),
                                },
                            },
                        })
                        .unwrap()
                }
            }
        }
    }

    fn main_loop(&mut self) {
        loop {
            let maybe_event = {
                let rx = self.rx.as_ref().unwrap();
                rx.try_recv()
            };

            if let Ok(ev) = maybe_event {
                let mut to_send: Option<Message<BroadcastPayload>> = None;
                match ev {
                    Event::Eof => break,
                    Event::TimeToGossip => {
                        self.do_gossiping();
                    }
                    Event::MessageReceived(msg) => {
                        match &msg.body.payload {
                            BroadcastPayload::Read {} => {
                                eprintln!("Received request to read");
                                let mut msgs = Vec::new();
                                for x in self.all_known.iter() {
                                    msgs.push(*x)
                                }

                                to_send = Some(msg.to_response(
                                    None,
                                    BroadcastPayload::ReadOk { messages: msgs },
                                ));
                            }
                            BroadcastPayload::Broadcast { message: x } => {
                                eprintln!("Received broadcast {}", x);
                                self.all_known.insert(*x);

                                to_send =
                                    Some(msg.to_response(None, BroadcastPayload::BroadcastOk {}));
                            }
                            BroadcastPayload::Topology { topology } => {
                                eprintln!("Received topology");
                                self.topology = Some(topology.clone());
                                to_send =
                                    Some(msg.to_response(None, BroadcastPayload::TopologyOk {}));
                            }
                            BroadcastPayload::Gossip { known } => {
                                eprintln!("Received gossiping");
                                for x in known.iter() {
                                    self.all_known.insert(*x);
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
                            let node_metadata = self.node_metadata.as_mut().unwrap();
                            let output = self.output.as_mut().unwrap();

                            msg.body.msg_id = Some(node_metadata.get_next_msg_id());
                            output.send_msg(msg).unwrap();
                        }
                    }
                }
            }
        }
    }

    fn start(&mut self) -> anyhow::Result<()> {
        let (node_metadata, _, output) = Maelstrom::init()?;
        let (tx, rx) = std::sync::mpsc::channel::<Event>();

        self.rx = Some(rx);
        self.output = Some(output);
        self.node_metadata = Some(node_metadata);

        let tx_clone = tx.clone();
        std::thread::spawn(move || read_stdin(tx_clone));
        std::thread::spawn(move || timer(tx));

        self.main_loop();
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::new();
    node.start()?;
    Ok(())
}
