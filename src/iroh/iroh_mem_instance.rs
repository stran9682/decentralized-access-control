use iroh::Endpoint;
use iroh_blobs::store::mem::MemStore;
use iroh_docs::protocol::Docs;

#[derive(Debug, Clone)]
pub struct IrohMemInstance {
    store: MemStore,
    docs: Docs,
    endpoint: Endpoint,
}

impl IrohMemInstance {
    pub fn new(store: MemStore, docs: Docs, endpoint: Endpoint) -> Self {
        Self {
            store,
            docs,
            endpoint,
        }
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
