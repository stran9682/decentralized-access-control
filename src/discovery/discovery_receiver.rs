use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use iroh::EndpointId;
use iroh_gossip::api::{Event, GossipReceiver};
use tokio::{
    sync::mpsc::UnboundedSender,
    time::{Instant, sleep},
};
use tokio_stream::StreamExt;

use crate::discovery::SignedMessage;

pub async fn update_map(
    receiver: &mut GossipReceiver,
    neighbors_last_seen: Arc<DashMap<EndpointId, Instant>>,
    peer_tx: UnboundedSender<EndpointId>,
) -> anyhow::Result<()> {
    while let Some(res) = receiver.next().await {
        match res {
            Ok(Event::Received(msg)) => {
                // Verify and decode the signed message
                let (verifying_key, address_info) =
                    match SignedMessage::verify_and_decode(&msg.content) {
                        Ok(result) => result,
                        Err(e) => {
                            eprintln!("Failed to verify message signature, ignoring {}", e);
                            continue;
                        }
                    };

                // Verify that the claimed node_id matches the public key
                let bytes: &[u8; 32] = verifying_key.as_bytes()[..].try_into()?;

                let expected_node_id = EndpointId::from_bytes(bytes)?;
                if address_info.node_id != expected_node_id {
                    println!("EndpointId spoofing attempt detected, ignoring message");
                    continue;
                }

                let is_new_peer = !neighbors_last_seen.contains_key(&expected_node_id);

                if is_new_peer {
                    // Send new peer to sender for joining
                    peer_tx.send(address_info.node_id)?;
                    println!("Discovered new peer");

                    todo!("Add to store")
                }

                neighbors_last_seen.insert(expected_node_id, Instant::now());
                println!("Address book updated, {}", neighbors_last_seen.len());
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error receiving gossip {}", e);
            }
        }
    }
    Ok(())
}

pub async fn start_cleanup_task(
    neighbor_map: Arc<DashMap<EndpointId, Instant>>,
    expiration_timeout: Duration,
) {
    let cleanup_interval = expiration_timeout / 3; // Check every 1/3 of timeout period

    loop {
        sleep(cleanup_interval).await;

        let now = Instant::now();
        let mut expired_count = 0;

        // Collect expired node names first to avoid holding locks
        let expired_nodes: Vec<EndpointId> = neighbor_map
            .iter()
            .filter_map(|entry| {
                if now.duration_since(*entry.value()) > expiration_timeout {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        // Remove expired nodes
        for endpoint_id in expired_nodes {
            if let Some(_) = neighbor_map.remove(&endpoint_id) {
                println!("Expired node: {}", endpoint_id);
                expired_count += 1;
            }
        }

        if expired_count > 0 {
            println!("Cleaned up expired nodes, {}", expired_count);
        }
    }
}
