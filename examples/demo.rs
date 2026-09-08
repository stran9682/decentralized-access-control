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
    let access_list_blobs = MemStore::new();
    let gossip = Gossip::builder().spawn(endpoint.clone());

    let docs = Docs::memory()
        .spawn(
            endpoint.clone(),
            (*access_list_blobs).clone(),
            gossip.clone(),
        )
        .await?;

    let acl_iroh_instance =
        IrohMemInstance::new(access_list_blobs.clone(), docs.clone(), endpoint.clone());

    let list_manager = AccessListManager::new(acl_iroh_instance);

    let storage_blobs = MemStore::new();
    let storage_iroh_instance = IrohMemInstance::new(storage_blobs, docs.clone(), endpoint.clone());
    let storage_manager = StorageManager::new(storage_iroh_instance);

    let access_control = AccessControl::new(list_manager.clone(), storage_manager, endpoint.id());

    println!("Endpoint: {}", endpoint.id());

    let _router = ARouter::builder(endpoint)
        .accept(DOCS_ALPN, docs)
        .accept(GOSSIP_ALPN, gossip)
        .accept(BLOBS_ALPN, BlobsProtocol::new(&access_list_blobs, None))
        .accept(ALPN, access_control.clone())
        .spawn();

    let _ = access_control.upload_new("demo_vid", "funny video").await?;

    tokio::signal::ctrl_c().await?;

    Ok(())
}
