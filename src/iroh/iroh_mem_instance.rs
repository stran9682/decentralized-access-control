use iroh::{Endpoint, endpoint::presets};
use iroh_blobs::store::mem::MemStore;
use iroh_docs::protocol::Docs;
use iroh_gossip::Gossip;

#[derive(Debug, Clone)]
pub struct IrohMemInstance {
    store: MemStore,
    docs: Docs,
    endpoint: Endpoint,
}

impl IrohMemInstance {
    pub async fn new() -> anyhow::Result<Self> {
        let endpoint = Endpoint::bind(presets::N0).await?;
        let blobs = MemStore::new();
        let gossip = Gossip::builder().spawn(endpoint.clone());

        let docs = Docs::memory()
            .spawn(endpoint.clone(), (*blobs).clone(), gossip)
            .await?;

        Ok(Self {
            store: blobs,
            docs,
            endpoint,
        })
    }
}

impl IrohMemInstance {
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn blobs(&self) -> &iroh_blobs::api::Store {
        &self.store
    }

    pub fn docs(&self) -> &Docs {
        &self.docs
    }
}
