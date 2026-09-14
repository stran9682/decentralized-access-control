use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

use anyhow::bail;
use dashmap::DashMap;
use ed25519_dalek::SigningKey;
use iroh::{Endpoint, EndpointId};
use iroh_gossip::{Gossip, TopicId};
use tokio::time::Instant;

use crate::discovery::{AddressInfo, discovery_receiver, discovery_sender::GossipDiscoverySender};

#[derive(Debug, Clone)]
pub struct DiscoveryService {
    endpoint: Endpoint,
    gossip: Gossip,

    /// I'm so sorry
    /// topic -> (EndpointId -> Last seen time)
    active_peers: Arc<DashMap<TopicId, Arc<DashMap<EndpointId, Instant>>>>,
}

impl DiscoveryService {
    pub fn new(endpoint: Endpoint, gossip: Gossip) -> Self {
        Self {
            endpoint,
            gossip,
            active_peers: Arc::new(DashMap::new()),
        }
    }

    pub async fn get_topic_peers(
        &self,
        topic_id: &str,
    ) -> anyhow::Result<Option<HashSet<EndpointId>>> {
        let topic_id = TopicId::from_str(topic_id)?;

        let Some(peers) = self.active_peers.get(&topic_id) else {
            return Ok(None);
        };

        let active_peers: HashSet<EndpointId> = peers.iter().map(|x| x.key().clone()).collect();

        return Ok(Some(active_peers));
    }

    pub async fn emit_topic(
        &self,
        topic_id: &str,
        bootstrap: Vec<EndpointId>,
    ) -> anyhow::Result<()> {
        let topic_id = TopicId::from_str(topic_id)?;

        if self.active_peers.contains_key(&topic_id) {
            bail!("Already subscribed to the gossip topic.")
        }

        // Setting up subscribing to the gossip topic.
        let (sender, mut receiver) = self.gossip.subscribe(topic_id, bootstrap).await?.split();

        let (peer_tx, peer_rx) = tokio::sync::mpsc::unbounded_channel::<EndpointId>();
        let neighbors_last_seen = Arc::new(DashMap::<EndpointId, Instant>::new());
        self.active_peers
            .insert(topic_id, Arc::clone(&neighbors_last_seen));

        let node_secret = self.endpoint.secret_key();
        let secret_key_bytes = node_secret.to_bytes();
        let secret_key = SigningKey::from_bytes(&secret_key_bytes);

        let address_info = AddressInfo {
            node_id: self.endpoint.id(),
            topic_id: topic_id.to_string(),
        };

        let mut discovery_sender = GossipDiscoverySender {
            peer_rx,
            sender,
            secret_key,
            address_info,
        };

        let neighbors_last_seen = Arc::clone(&neighbors_last_seen);
        tokio::spawn(async move {
            tokio::select! {
                Err(e) = discovery_sender.gossip(Duration::from_secs(2)) => {
                    eprintln!("The signed message was invalid and couldn't be sent: {}", e)
                }
                _ = discovery_receiver::start_cleanup_task(
                    Arc::clone(&neighbors_last_seen),
                    Duration::from_secs(30),
                ) => {},
                res = discovery_receiver::update_map(&mut receiver, neighbors_last_seen, peer_tx)  => {
                    match res {
                        Ok(_) => { println!("The gossip receiver is no longer receiving messages"); },
                        Err(e) => { eprintln!("An error occured during peer discovery: {}", e)}
                    };
                }
            }
        });

        Ok(())
    }
}
