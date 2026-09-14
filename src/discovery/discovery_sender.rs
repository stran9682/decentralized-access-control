use std::time::Duration;

use ed25519_dalek::SigningKey;
use iroh::EndpointId;
use iroh_gossip::api::GossipSender;
use tokio::{sync::mpsc::UnboundedReceiver, time::sleep};

use crate::discovery::{AddressInfo, SignedMessage};

pub struct GossipDiscoverySender {
    pub peer_rx: UnboundedReceiver<EndpointId>,
    pub sender: GossipSender,
    pub secret_key: SigningKey,
    pub address_info: AddressInfo,
}

impl GossipDiscoverySender {
    pub async fn gossip(&mut self, update_rate: Duration) -> anyhow::Result<()> {
        loop {
            // Check for new peers to join
            match self.peer_rx.try_recv() {
                Ok(peer) => {
                    println!("Joining new peer {}", peer);
                    if let Err(e) = self.sender.join_peers(vec![peer]).await {
                        eprintln!("Failed to join peer {}", e);
                    }
                }
                Err(_) => {}
            }

            // Sign and encode the message
            let bytes = SignedMessage::sign_and_encode(&self.secret_key, &self.address_info)?;

            if let Err(e) = self.sender.broadcast(bytes).await {
                eprintln!("Failed to broadcast {}", e);
            }

            sleep(update_rate).await;
        }
    }
}
