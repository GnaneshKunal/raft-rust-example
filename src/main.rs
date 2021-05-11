#[macro_use]
extern crate log;

use std::{net::UdpSocket, thread, time::Duration};

use raft::{
    broadcast_msg, process_msg, receive_msg, send_ping, Action, Message, Port, RaftConsensus,
    RaftState, Response,
};

fn transmit_heartbeat(ports: Vec<Port>) {
    let raft = RaftConsensus::from(ports);

    let port = raft.inner.read().unwrap().port;
    info!("Listening on port: {}", port);

    let socket = UdpSocket::bind(format!("127.0.0.1:{}", port)).unwrap();

    let handler1 = thread::spawn({
        let raft = raft.inner.clone();
        let socket = socket.try_clone().unwrap();
        move || loop {
            let my_port = raft.read().unwrap().port;
            let leader = raft.read().unwrap().leader_port.clone();

            if leader.is_some() {
                if leader.unwrap() == my_port {
                    send_ping(&socket, &raft);
                }
            }

            thread::sleep(Duration::from_secs(2));
        }
    });

    let handler2 = thread::spawn({
        let raft = raft.inner.clone();
        let socket = socket.try_clone().unwrap();
        move || loop {
            let response = receive_msg(&socket);
            match response {
                Response::Data(port, message) => {
                    info!("Data: {:?}", message);
                    process_msg(message, &raft, &socket, port);
                }
                Response::Error(port, err_str) => {
                    error!("Error: {:?} from {:?}", err_str, port.map(|p| p));
                }
            }
        }
    });

    let leader_election_thread = thread::spawn({
        let raft = raft.inner.clone();
        let socket = socket.try_clone().unwrap();
        move || loop {
            let now = chrono::Local::now();
            let node_time = raft.read().unwrap().timer.unwrap();
            let diff = now - node_time;
            let my_state = raft.read().unwrap().state;

            let new_stage = raft.read().unwrap().stage + 1;
            if diff.num_seconds() >= 5
                && my_state != RaftState::LeaderRequestApprovalWaiting
            // [IMPORTANT] TODO:
            // my_state can be `LeaderRequestSent`, for re-election
            // when both nodes tie.
                && my_state != RaftState::LeaderRequestSent
                && my_state != RaftState::Leader
            {
                debug!("Trying for leader request {:?}", my_state);

                raft.write().unwrap().clear_approval_nodes();
                raft.write().unwrap().set_state(RaftState::Initial);

                let members: Vec<String> = raft
                    .read()
                    .unwrap()
                    .members
                    .iter()
                    .map(|port| format!("127.0.0.1:{}", port))
                    .collect();
                let message = Message::Payload(Action::LeaderRequest, new_stage);

                broadcast_msg(&socket, &members[..], message);

                // if the above one was successful.
                raft.write()
                    .unwrap()
                    .set_state(RaftState::LeaderRequestSent);
                // send_msg(socket: &UdpSocket, to_address: String, msg: Message)
            } else {
                // println!("Sleeping: {:?}", my_state);
                println!(
                    "Sleeeping: \
                          State: {:?} \
                          Stage: {:?} ",
                    my_state,
                    raft.read().unwrap().stage
                );
                thread::sleep(Duration::from_secs(1));
            }
        }
    });

    handler1.join().unwrap();
    handler2.join().unwrap();
    leader_election_thread.join().unwrap();
}

fn main() {
    env_logger::init();

    let args: Vec<Port> = std::env::args()
        .skip(1)
        .map(|v| v.parse().unwrap())
        .collect();

    transmit_heartbeat(args);
}
