use std::{
    collections::HashMap,
    process::exit,
    sync::mpsc::{Receiver, Sender},
};

use anyhow::Ok;
use node_driver::{Body, InputInterface, Maelstrom, Message, NodeMetadata, OutputInterface};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum KafkaMessage {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },

    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, usize)>>,
    },

    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk {},

    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

enum Event {
    MessageReceived(Message<KafkaMessage>),
    EndOfFile,
}

struct Node {
    offsets: HashMap<String, usize>,
    queues: HashMap<String, Vec<usize>>,

    rx: Option<Receiver<Event>>,
    node_metadata: Option<NodeMetadata>,
    output: Option<OutputInterface>,
}

impl Node {
    fn new() -> Node {
        Node {
            offsets: HashMap::new(),
            queues: HashMap::new(),

            rx: None,
            node_metadata: None,
            output: None,
        }
    }

    fn read_stdin(tx: Sender<Event>) -> anyhow::Result<()> {
        let mut input = InputInterface::default();

        for msg in input.iter::<KafkaMessage>() {
            let msg = msg?;
            tx.send(Event::MessageReceived(msg)).unwrap();
        }

        tx.send(Event::EndOfFile).unwrap();
        Ok(())
    }

    fn enqueue(&mut self, log: &String, value: usize) -> usize {
        let maybe_values = self.queues.get_mut(log);
        if let Some(values) = maybe_values {
            values.push(value);
            *self.offsets.get_mut(log).unwrap() += 1;
            return self.queues.get(log).unwrap().len() - 1;
        }

        self.queues.insert(log.clone(), vec![value]);
        self.offsets.insert(log.clone(), 0);
        0
    }

    fn process_message(&mut self, msg: &Message<KafkaMessage>) -> Option<Message<KafkaMessage>> {
        let payload = &msg.body.payload;
        match &payload {
            KafkaMessage::Send { key, msg: value } => {
                let offset = self.enqueue(key, *value);

                let metadata = self.node_metadata.as_mut().unwrap();
                eprintln!(
                    "Enqueued into {} value {}, new offset {}",
                    key, value, offset
                );
                Some(Message {
                    src: metadata.node_id.clone(),
                    dst: msg.src.clone(),
                    body: Body {
                        msg_id: Some(metadata.get_next_msg_id()),
                        in_reply_to: msg.body.msg_id,
                        payload: KafkaMessage::SendOk { offset },
                    },
                })
            }
            KafkaMessage::Poll { offsets } => {
                let metadata = self.node_metadata.as_mut().unwrap();

                let mut result: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
                for (key, offset) in offsets.iter() {
                    eprintln!("Reading from {} at offset {}", key, offset);
                    let queue = self.queues.get(key);
                    if queue.is_none() {
                        break;
                    }
                    let queue = queue.unwrap();

                    if *offset >= queue.len() {
                        break;
                    }

                    let val = *queue.get(*offset).unwrap();
                    result.insert(key.clone(), vec![(*offset, val)]);
                    eprintln!("-> {}: {}, {}", key.clone(), *offset, val);
                }

                Some(Message {
                    src: metadata.node_id.clone(),
                    dst: msg.src.clone(),
                    body: Body {
                        msg_id: Some(metadata.get_next_msg_id()),
                        in_reply_to: msg.body.msg_id,
                        payload: KafkaMessage::PollOk { msgs: result },
                    },
                })
            }
            KafkaMessage::CommitOffsets { offsets } => {
                let metadata = self.node_metadata.as_mut().unwrap();

                for (key, offset) in offsets {
                    *self.offsets.get_mut(key).unwrap() = *offset;
                }

                Some(Message {
                    src: metadata.node_id.clone(),
                    dst: msg.src.clone(),
                    body: Body {
                        msg_id: Some(metadata.get_next_msg_id()),
                        in_reply_to: msg.body.msg_id,
                        payload: KafkaMessage::CommitOffsetsOk {},
                    },
                })
            }
            KafkaMessage::ListCommittedOffsets { keys } => {
                let metadata = self.node_metadata.as_mut().unwrap();

                let mut offsets = HashMap::new();
                eprintln!("Reading committed offsets");
                for key in keys {
                    let offset = self.offsets.get(key).unwrap_or(&0);
                    offsets.insert(key.clone(), *offset);
                    eprintln!("-> {}: {}", key, offset);
                }

                Some(Message {
                    src: metadata.node_id.clone(),
                    dst: msg.src.clone(),
                    body: Body {
                        msg_id: Some(metadata.get_next_msg_id()),
                        in_reply_to: msg.body.msg_id,
                        payload: KafkaMessage::ListCommittedOffsetsOk { offsets },
                    },
                })
            }
            _ => {
                panic!("Unexpected message type received!");
            }
        }
    }

    fn main_loop(&mut self) {
        loop {
            let msg = {
                let rx = { self.rx.as_ref().unwrap() };
                rx.try_recv()
            };

            if let std::result::Result::Ok(message) = &msg {
                match &message {
                    Event::MessageReceived(msg) => {
                        let maybe_message = self.process_message(msg);
                        if let Some(msg) = maybe_message {
                            let output = self.output.as_mut();
                            let result = output.unwrap().send_msg(msg);
                            if result.is_err() {
                                panic!("Couldn't send message");
                            }
                        }
                    }
                    Event::EndOfFile => {
                        exit(5);
                        // break;
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

        let tx_clone = tx;
        let handle = std::thread::spawn(move || Node::read_stdin(tx_clone));
        self.main_loop();

        let _ = handle.join();
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::new();
    node.start()?;
    Ok(())
}
