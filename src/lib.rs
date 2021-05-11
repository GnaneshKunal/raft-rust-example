#[macro_use]
extern crate log;

use std::{
    collections::BTreeSet,
    net::UdpSocket,
    sync::{Arc, RwLock},
};

use chrono::Local;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

pub type Stage = u8;
pub type Port = u16;
pub const RAFT_TIMER: u8 = 10;

pub type Members = Vec<Port>;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RaftState {
    // Initial state. Send leader request as soon as timesout.
    Initial,

    // Sent leader request, waiting for the nodes to accept.
    LeaderRequestSent,

    // Approved a leader request already
    LeaderRequestApprovalWaiting,

    // Mostly same as initial, monitoring leader.
    Follower,

    // leader state
    Leader,
    // TODO:
    // Add more state for consensus
}

#[derive(Debug)]
pub struct Raft {
    pub members: Members,
    pub port: Port,
    pub leader_port: Option<Port>,
    logs: Vec<String>,
    pub stage: Stage,
    pub timer: Option<chrono::DateTime<Local>>,
    pub state: RaftState,
    approval_nodes: BTreeSet<Port>,
}

pub struct RaftConsensus {
    pub inner: Arc<RwLock<Raft>>,
}

impl Raft {
    pub fn new(port: Port) -> Self {
        let mut rng = thread_rng();

        Self {
            members: vec![],
            port: port,
            leader_port: None,
            logs: vec![],
            stage: 0,
            // now + random(5, 10)
            timer: Some(chrono::Local::now() + chrono::Duration::seconds(rng.gen_range(5..10))),
            state: RaftState::Initial,
            approval_nodes: BTreeSet::new(),
        }
    }

    pub fn add_member(&mut self, member: Port) {
        self.members.push(member)
    }

    pub fn reset_timer(&mut self) {
        let timer = self.timer.as_mut().unwrap();

        let mut rng = thread_rng();
        *timer = chrono::Local::now() + chrono::Duration::seconds(rng.gen_range(5..8));
    }

    pub fn set_leader(&mut self, leader: Port) {
        self.leader_port = Some(leader);
    }

    pub fn clear_leader(&mut self) {
        self.leader_port = None;
    }

    pub fn set_state(&mut self, state: RaftState) {
        debug!("Changing state from {:?} to {:?}", self.state, state);
        self.state = state;
    }

    pub fn get_state(&self) -> RaftState {
        self.state
    }

    pub fn clear_approval_nodes(&mut self) {
        self.approval_nodes.clear();
    }

    pub fn append_approval_node(&mut self, port: Port) {
        self.approval_nodes.insert(port);
    }

    pub fn set_stage(&mut self, stage: Stage) {
        self.stage = stage;
    }
}

impl RaftConsensus {
    pub fn new(port: Port) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Raft::new(port))),
        }
    }

    pub fn add_member(&mut self, member: Port) {
        self.inner.write().unwrap().members.push(member)
    }

    pub fn reset_timer(&mut self) {
        let mut raft = self.inner.write().unwrap();

        raft.reset_timer();
    }
}

impl From<Vec<Port>> for RaftConsensus {
    fn from(members: Vec<Port>) -> Self {
        let mut raft_consensus = Self::new(*members.first().unwrap());

        for member in &members[1..] {
            raft_consensus.add_member(*member);
        }

        raft_consensus
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum LeaderRequestApproval {
    Accept(Stage),
    Reject(Stage),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Message {
    Payload(Action, Stage),
    Error(String),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Action {
    Ping,
    Pong,
    LeaderRequest,
    LeaderResponse(LeaderRequestApproval),
    LeaderChange,
    LeaderChangeReport,
    Error(String),
}

#[derive(Debug)]
pub enum Response {
    Data(Port, Message),
    Error(Option<Port>, Message),
}

pub fn send_ping(socket: &UdpSocket, raft: &Arc<RwLock<Raft>>) {
    let members: Vec<String> = raft
        .read()
        .unwrap()
        .members
        .iter()
        .map(|port| format!("127.0.0.1:{}", port))
        .collect();

    let ping = Message::Payload(Action::Ping, raft.read().unwrap().stage);

    broadcast_msg(socket, &members[..], ping);
}

pub fn broadcast_msg(socket: &UdpSocket, addresses: &[String], msg: Message) {
    debug!("Broadcasted {:?}", msg);
    for address in addresses {
        send_msg(socket, address.to_string(), msg.clone());
    }
}

pub fn send_msg(socket: &UdpSocket, to_address: String, msg: Message) {
    let msg_encoded = bincode::serialize(&msg).unwrap();
    debug!("Sending {:?} to {:?}", msg, to_address);
    socket.send_to(&msg_encoded, to_address).unwrap();
}

pub fn receive_msg(socket: &UdpSocket) -> Response {
    let mut buf = [0u8; 1024];
    socket.set_read_timeout(None).unwrap();

    match socket.recv_from(&mut buf) {
        Ok((_amt, src_addr)) => {
            let message: Message = bincode::deserialize(&buf).unwrap();
            info!("Received {:?} from {:?}", message, src_addr.port());
            Response::Data(src_addr.port(), message)
        }
        Err(err) => Response::Error(None, Message::Error(err.to_string())),
    }
}

pub fn process_msg(
    message: Message,
    raft: &Arc<RwLock<Raft>>,
    socket: &UdpSocket,
    node_port: Port,
) {
    let leader_port = raft.read().unwrap().leader_port;
    match message {
        Message::Payload(action, stage) => {
            let my_stage = raft.read().unwrap().stage;
            let my_state = raft.read().unwrap().state;
            let my_port = raft.read().unwrap().port;
            let to_address = format!("127.0.0.1:{}", node_port);

            match action {
                Action::Ping => {
                    // [X] Reply (PONG) only for leader PING messages
                    // [ ] Also change leader if necessary
                    // [ ] Should also sync if lagging (i.e, get logs)

                    if stage < my_stage {
                        // TODO: request node has outdated
                        todo!();
                    } else if stage > my_stage {
                        // TODO: I'm lagging
                        todo!();
                    } else if leader_port.is_none() {
                        // TODO: how could this be possible. Only a leader
                        // request should be considered. Could the node ever
                        // be in a state such that its leader is `None`?
                        todo!();
                    } else if leader_port.is_some() {
                        // There's leader and he just wants to tell you that
                        // he's alive.
                        raft.write().unwrap().reset_timer();
                        send_msg(socket, to_address, Message::Payload(Action::Pong, stage));
                    }
                }
                Action::Pong => {
                    // Pong should be accepted only when I'm the leader.  This
                    // has consensus use cases apart from leader election -
                    // `Approval` for replication (one use case).

                    // These cases should be handled.
                    if stage < my_stage {
                        // TODO: request node has outdated
                        todo!();
                    } else if stage > my_stage {
                        // TODO: I'm lagging
                        todo!();
                    } else if leader_port.is_none() {
                        // TODO: how could this be possible. Only a leader
                        // request should be considered. Could the node ever
                        // be in a state such that its leader is `None`?
                        todo!();
                    }

                    // TODO: Currently don't do anything since there's
                    // nothing to do in leader election. (You can try
                    // failure detection in raft though.)
                    info!("Received PONG({:?} from {:?}", stage, node_port);
                }
                Action::LeaderRequest => {
                    // [X] If a new stage is proposed, accept the leader
                    // request (eventually all the nodes will accept).
                    //
                    // [ ] if the stage is same, ignore. This should never
                    // happen though. Notify reject the leadership (should you
                    // also send leader port?) or will it be accepted by PING?
                    //
                    // [ ] If old stage, notify the new stage (should you
                    // notify the leader as well)? or will it be accepted by
                    // PING?

                    debug!(
                        "Leader request for: {} and my stage is: {}",
                        stage, my_stage
                    );
                    if stage <= my_stage {
                        send_msg(
                            socket,
                            to_address,
                            Message::Payload(
                                Action::LeaderResponse(LeaderRequestApproval::Reject(my_stage)),
                                my_stage,
                            ),
                        );
                    } else {
                        if let RaftState::LeaderRequestApprovalWaiting = my_state {
                            debug!("Rejecting. Accepted another node.");
                            // [IMPROVEMENT] TODO: Reject saying that you have already approved.
                            send_msg(
                                socket,
                                to_address,
                                Message::Payload(
                                    Action::LeaderResponse(LeaderRequestApproval::Reject(my_stage)),
                                    my_stage,
                                ),
                            );
                        } else if let RaftState::LeaderRequestSent = my_state {
                            debug!("Rejecting since even I'm in the election");
                            // [IMPROVEMENT] TODO: Reject saying that you are in the election.
                            send_msg(
                                socket,
                                to_address,
                                Message::Payload(
                                    Action::LeaderResponse(LeaderRequestApproval::Reject(my_stage)),
                                    my_stage,
                                ),
                            );
                        } else {
                            debug!("Approving node: {}", node_port);
                            send_msg(
                                socket,
                                to_address,
                                Message::Payload(
                                    Action::LeaderResponse(
                                        // accepting proposed `stage`
                                        LeaderRequestApproval::Accept(stage),
                                    ),
                                    my_stage,
                                ),
                            );
                            // The below should succeed only if the above
                            // function won't return any error
                            // message. basically you should catch the
                            // socket error. But its UDP??
                            raft.write()
                                .unwrap()
                                .set_state(RaftState::LeaderRequestApprovalWaiting);
                        }
                    }
                }
                Action::LeaderResponse(request_approval) => {
                    // You have sent a leader request, you should accept only
                    // if more than `f` nodes have accepted the leader
                    // request. `f` in `2f + 1`.
                    //
                    // If less than `f` nodes have accepted the request, have
                    // a random backoff before sending a leader request again
                    // with bigger stage.  Also, while in the backoff, if
                    // another node sends a higer stage than current stage,
                    // accept it and don't participate in leader election.

                    // This should be next stage. Your current stage +
                    // 1.  You have sent a proposal with next stage,
                    // so you should check the approval stage and your
                    // `should_be_next_stage` is same.
                    let should_be_next_stage = my_stage + 1;
                    match request_approval {
                        LeaderRequestApproval::Accept(approval_stage) => {
                            if approval_stage < my_stage {
                                // Probably old approval. Need to sync.
                                todo!();
                            } else if approval_stage == (should_be_next_stage) {
                                raft.write().unwrap().append_approval_node(node_port);
                            } else if approval_stage > my_stage {
                                // This is impossible.
                                todo!();
                            } else if approval_stage == my_stage {
                                // This is imposible too.
                                debug!("Rejecting {} since even I'm in the election", node_port);
                                // [IMPROVEMENT] TODO: Reject saying that you are in the election.
                                send_msg(
                                    socket,
                                    to_address,
                                    Message::Payload(
                                        Action::LeaderResponse(LeaderRequestApproval::Reject(
                                            my_stage,
                                        )),
                                        my_stage,
                                    ),
                                );
                                return;
                            }

                            if my_state != RaftState::Leader {
                                let members_len = raft.read().unwrap().members.len();
                                let approval_nodes_len = raft.read().unwrap().approval_nodes.len();
                                if approval_nodes_len >= (members_len - 1) / 2 {
                                    // more than `f` nodes have approved
                                    let members: Vec<String> = raft
                                        .read()
                                        .unwrap()
                                        .members
                                        .iter()
                                        .map(|port| format!("127.0.0.1:{}", port))
                                        .collect();
                                    let message = Message::Payload(
                                        Action::LeaderChange,
                                        should_be_next_stage,
                                    );
                                    broadcast_msg(socket, &members[..], message);
                                }
                                debug!("Becoming leader");
                                let mut raft = raft.write().unwrap();
                                raft.set_leader(my_port);
                                raft.clear_approval_nodes();
                                raft.set_state(RaftState::Leader);
                                raft.set_stage(should_be_next_stage);
                            }
                        }
                        LeaderRequestApproval::Reject(reject_stage) => {
                            if reject_stage <= my_stage {
                                // Probably the node has accepted other node's request.
                            } else {
                                // I'm lagging;
                                // TODO:
                                raft.write().unwrap().clear_approval_nodes();
                            }

                            // The next election will be after the timeout.
                        }
                    }
                }
                Action::LeaderChange => {
                    // Some node has become leader.

                    if stage < my_stage {
                        // I'm not leader and found a request from
                        // outdated node that has considered itself a
                        // leader, try to report it. It's upto the
                        // node to handle it properly else it should
                        // be restarted.
                        send_msg(
                            socket,
                            to_address,
                            Message::Payload(Action::LeaderChangeReport, stage),
                        );
                    }

                    let mut raft = raft.write().unwrap();
                    raft.set_leader(node_port);
                    raft.clear_approval_nodes();
                    raft.reset_timer();
                    raft.set_state(RaftState::Follower);
                    raft.set_stage(stage);
                }
                Action::LeaderChangeReport => {
                    // I'm outdated
                    if stage > my_stage {
                        let mut raft = raft.write().unwrap();
                        raft.clear_leader();
                        raft.clear_approval_nodes();
                        raft.set_state(RaftState::Initial);
                    }
                }
                Action::Error(err_msg) => error!("Error: {:?}", err_msg),
            }
        }
        Message::Error(err_msg) => error!("Something went wrong: {:?}", err_msg),
    }
}

// Basic structs
//
// converting args to struct objs
//
// Simple message transfer between nodes
//
// make the struct inner since Arc and RwLock is needed to share
// between multiple threads.
//
// start implementing leader election and put stage in everything
// (i.e., every struct)
//
//
