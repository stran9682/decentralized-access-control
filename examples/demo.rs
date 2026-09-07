use decentralized_access_control::{
    ALPN, access_list::list_manager::AccessListManager, iroh::iroh_mem_instance::IrohMemInstance,
    protocol::access_control::AccessControl, store::storage_manager::StorageManager,
};
use iroh::{Endpoint, endpoint::presets, protocol::Router as ARouter};
use iroh_blobs::{ALPN as BLOBS_ALPN, BlobsProtocol, store::mem::MemStore};
use iroh_docs::{ALPN as DOCS_ALPN, protocol::Docs};
use iroh_gossip::{ALPN as GOSSIP_ALPN, Gossip};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    let blobs = MemStore::new();
    let gossip = Gossip::builder().spawn(endpoint.clone());

    let docs = Docs::memory()
        .spawn(endpoint.clone(), (*blobs).clone(), gossip.clone())
        .await?;

    let iroh_instance = IrohMemInstance::new(blobs.clone(), docs.clone(), endpoint);

    let list_manager = AccessListManager::new(iroh_instance.clone());
    let storage_manager = StorageManager::new(iroh_instance.clone());

    let access_control = AccessControl::new(
        list_manager.clone(),
        storage_manager,
        iroh_instance.endpoint().id(),
    );

    println!("Endpoint: {}", iroh_instance.endpoint().id());

    let _router = ARouter::builder(iroh_instance.endpoint().clone())
        .accept(DOCS_ALPN, iroh_instance.docs().clone())
        .accept(GOSSIP_ALPN, gossip)
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blobs, None))
        .accept(ALPN, access_control.clone())
        .spawn();

    let _ = access_control.upload_new("demo_vid", "funny video").await?;

    tokio::signal::ctrl_c().await?;

    Ok(())
}
