use async_std::prelude::*;
use async_std::net::{SocketAddr, TcpListener, TcpStream};
use async_std::task;
use async_std::sync::Arc;
use rand::RngCore;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use zenoh_protocol::core::PeerId;
use zenoh_protocol::io::{RBuf, WBuf};
use zenoh_protocol::proto::{SessionBody, SessionMessage};


async fn handle_client(mut stream: TcpStream, bs: usize, length: bool) -> Result<(), Box<dyn std::error::Error>> {
    let counter = Arc::new(AtomicUsize::new(0));

    let mut apid = vec![0, 0, 0, 0];
    rand::thread_rng().fill_bytes(&mut apid);
    let apid = PeerId{ id: apid };

    let mut buffer = vec![0u8; bs];

    stream.read(&mut buffer).await?;
    let mut rbuf = RBuf::from(&buffer[..]);
    // Skip the first two bytes
    if length {
        rbuf.read()?;
        rbuf.read()?;
    }
    
    let message = rbuf.read_session_message()?;

    match message.get_body() {
        SessionBody::Open { version: _, whatami, pid, lease: _, initial_sn, sn_resolution: _, locators: _ } => {            
            let opid = pid.clone();
            let sn_resolution = None;   
            let lease = None;
            let locators = None;
            let attachment = None;
            let message = SessionMessage::make_accept(*whatami, opid, apid, *initial_sn, sn_resolution, lease, locators, attachment);

            let mut wbuf = WBuf::new(32, false);        
            // Reserve 16 bits to write the lenght
            if length {
                wbuf.write_bytes(&[0u8, 0u8]); 
            }
            wbuf.write_session_message(&message);
            if length {
                let length: u16 = wbuf.len() as u16 - 2;
                // Write the length on the first 16 bits
                let bits = wbuf.get_first_slice_mut(..2);
                bits.copy_from_slice(&length.to_le_bytes());
            }

            let bf = wbuf.get_first_slice(..);
            stream.write_all(bf).await?;
        },
        _ => return Ok(())
    }

    let c_c = counter.clone();
    task::spawn(async move {
        loop {
            task::sleep(Duration::from_secs(1)).await;
            let c = c_c.swap(0, Ordering::Relaxed);
            println!("{:.3} Gbit/s", (8 as f64*c as f64)/1_000_000_000 as f64);
        }
    });

    loop {
        let n = stream.read(&mut buffer).await?;
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

async fn run(addr: SocketAddr, bs: usize, length: bool) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();

    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        task::spawn(async move {
            let _ = handle_client(stream, bs, length).await;
        });
    }

    Ok(())
}

fn main() {    
    let mut args = std::env::args();
    // Get exe name
    let _ = args.next().unwrap();
    
    // Get next arg
    let addr: SocketAddr = args.next().unwrap().parse().unwrap();
    let bs: usize = args.next().unwrap().parse().unwrap();
    let length: bool = args.next().unwrap().parse().unwrap();

    let _ = task::block_on(run(addr, bs, length));
}