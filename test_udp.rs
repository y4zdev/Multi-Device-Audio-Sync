use std::net::UdpSocket;

fn main() {
    println!("Binding to 0.0.0.0:30690...");
    let socket = UdpSocket::bind("0.0.0.0:30690").expect("Failed to bind UDP port");
    println!("Listening on UDP 30690... Send a packet from your local machine to 173.234.15.93:30690");
    let mut buf = [0; 1024];
    match socket.recv_from(&mut buf) {
        Ok((amt, src)) => {
            let msg = String::from_utf8_lossy(&buf[..amt]);
            println!("✅ SUCCESS! Received UDP packet from {}: {}", src, msg);
        }
        Err(e) => {
            println!("❌ Error receiving: {}", e);
        }
    }
}
