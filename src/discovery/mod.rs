use bytes::Bytes;
use dashmap::DashMap;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};

use iroh::EndpointId;
use iroh_gossip::api::{Event, GossipReceiver, GossipSender};
use iroh_gossip::{net::Gossip, proto::TopicId};

use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use std::sync::Arc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::{Duration, Instant, sleep};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Node {
    pub name: String,
    pub node_id: EndpointId,
    pub count: u32,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: EndpointId,
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SignedMessage {
    from: VerifyingKey,
    data: Bytes,
    signature: Signature,
}

impl SignedMessage {
    pub fn sign_and_encode(secret_key: &SigningKey, node: &Node) -> anyhow::Result<Bytes> {
        let data: Bytes = Bytes::from(serde_json::to_vec(node)?);
        let signature = secret_key.sign(&data);
        let from: VerifyingKey = secret_key.verifying_key();

        let signed_message = Self {
            from,
            data,
            signature,
        };

        let encoded = serde_json::to_vec(&signed_message)?;
        Ok(encoded.into())
    }

    pub fn verify_and_decode(bytes: &[u8]) -> anyhow::Result<(VerifyingKey, Node)> {
        let signed_message: Self = serde_json::from_slice(bytes)?;
        let key: VerifyingKey = signed_message.from;

        key.verify(&signed_message.data, &signed_message.signature)?;

        let node: Node = serde_json::from_slice(&signed_message.data)?;
        Ok((signed_message.from, node))
    }
}

pub struct GossipDiscoveryBuilder {
    expiration_timeout: Option<Duration>,
}

impl GossipDiscoveryBuilder {
    pub fn new() -> Self {
        Self {
            expiration_timeout: None,
        }
    }

    pub fn with_expiration_timeout(mut self, timeout: Duration) -> Self {
        self.expiration_timeout = Some(timeout);
        self
    }

    pub async fn build_with_peers(
        self,
        gossip: Gossip,
        topic_id: TopicId,
        peers: Vec<EndpointId>,
        endpoint: &iroh::Endpoint,
    ) -> anyhow::Result<(GossipDiscoverySender, GossipDiscoveryReceiver)> {
        // - First node (empty peers): use subscribe() only
        // - Other nodes (with peers): use subscribe_and_join()
        println!("Attempting to subscribe to gossip topic");
        let (sender, receiver) = gossip.subscribe(topic_id, peers).await?.split();
        println!("Subscribed to gossip topic");

        let (peer_tx, peer_rx) = tokio::sync::mpsc::unbounded_channel();
        let neighbor_map = Arc::new(DashMap::new());

        // Derive a secret key from the endpoint's node secret key
        // This ensures the signing key corresponds to the node's identity
        let node_secret = endpoint.secret_key();
        let secret_key_bytes = node_secret.to_bytes();
        let secret_key = SigningKey::from_bytes(&secret_key_bytes);
        let discovery_sender = GossipDiscoverySender {
            peer_rx,
            sender,
            secret_key,
        };

        let expiration_timeout = self.expiration_timeout.unwrap_or(Duration::from_secs(30));

        let discovery_receiver = GossipDiscoveryReceiver {
            neighbor_map: Arc::clone(&neighbor_map),
            peer_tx,
            receiver,
            expiration_timeout,
        };

        // Start the cleanup task
        GossipDiscoveryReceiver::start_cleanup_task(neighbor_map, expiration_timeout);

        Ok((discovery_sender, discovery_receiver))
    }
}

pub struct GossipDiscoverySender {
    pub peer_rx: UnboundedReceiver<EndpointId>,
    pub sender: GossipSender,
    pub secret_key: SigningKey,
}

impl GossipDiscoverySender {
    /// Add external peers to the gossip network
    pub async fn add_peers(&mut self, peers: Vec<EndpointId>) -> anyhow::Result<()> {
        if !peers.is_empty() {
            println!("Adding external peers to gossip network");
            self.sender.join_peers(peers).await?;
        }
        Ok(())
    }

    /// Add a single external peer to the gossip network  
    pub async fn add_peer(&mut self, peer: EndpointId) -> anyhow::Result<()> {
        self.add_peers(vec![peer]).await
    }

    pub async fn gossip(&mut self, node: Node, update_rate: Duration) -> anyhow::Result<()> {
        let mut i = node.count;

        loop {
            // Check for new peers to join
            match self.peer_rx.try_recv() {
                Ok(peer) => {
                    println!("Joining new peer {}", peer);
                    if let Err(e) = self.sender.join_peers(vec![peer]).await {
                        eprint!("Failed to join peer {}", e);
                    }
                }
                Err(_) => {}
            }

            let update_node = Node {
                name: node.name.clone(),
                node_id: node.node_id,
                count: i,
            };

            // Sign and encode the message
            let bytes = SignedMessage::sign_and_encode(&self.secret_key, &update_node)?;

            if let Err(e) = self.sender.broadcast(bytes).await {
                eprintln!("Failed to broadcast {}", e);
            }

            i += 1;
            sleep(update_rate).await;
        }
    }
}

pub struct GossipDiscoveryReceiver {
    pub neighbor_map: Arc<DashMap<String, NodeInfo>>,
    pub peer_tx: UnboundedSender<EndpointId>,
    pub receiver: GossipReceiver,
    pub expiration_timeout: Duration,
}

impl GossipDiscoveryReceiver {
    pub async fn update_map(&mut self) -> anyhow::Result<()> {
        while let Some(res) = self.receiver.next().await {
            match res {
                Ok(Event::Received(msg)) => {
                    // Verify and decode the signed message
                    let (verifying_key, value) =
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
                    if value.node_id != expected_node_id {
                        println!("EndpointId spoofing attempt detected, ignoring message");
                        continue;
                    }

                    let is_new_peer = !self.neighbor_map.contains_key(&value.name);

                    if is_new_peer {
                        // Send new peer to sender for joining
                        self.peer_tx.send(value.node_id)?;
                        println!("Discovered new peer");
                    }

                    self.neighbor_map.insert(
                        value.name.clone(),
                        NodeInfo {
                            node_id: value.node_id,
                            last_seen: Instant::now(),
                        },
                    );
                    println!("Address book updated, {}", self.neighbor_map.len());
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Error receiving gossip {}", e);
                }
            }
        }
        Ok(())
    }

    pub fn get_neighbors(&self) -> Vec<(String, EndpointId)> {
        self.neighbor_map
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().node_id))
            .collect()
    }

    pub fn start_cleanup_task(
        neighbor_map: Arc<DashMap<String, NodeInfo>>,
        expiration_timeout: Duration,
    ) {
        let cleanup_interval = expiration_timeout / 3; // Check every 1/3 of timeout period

        tokio::spawn(async move {
            loop {
                sleep(cleanup_interval).await;

                let now = Instant::now();
                let mut expired_count = 0;

                // Collect expired node names first to avoid holding locks
                let expired_nodes: Vec<String> = neighbor_map
                    .iter()
                    .filter_map(|entry| {
                        if now.duration_since(entry.value().last_seen) > expiration_timeout {
                            Some(entry.key().clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                // Remove expired nodes
                for node_name in expired_nodes {
                    if let Some((_, node_info)) = neighbor_map.remove(&node_name) {
                        println!("Expired node: {}, {}", node_name, node_info.node_id);
                        expired_count += 1;
                    }
                }

                if expired_count > 0 {
                    println!("Cleaned up expired nodes, {}", expired_count);
                }
            }
        });
    }
}
