use decentralized_access_control::{
    ALPN, access_list::list_manager::AccessListManager, iroh::iroh_mem_instance::IrohMemInstance,
    protocol::access_control::AccessControl, store::storage_manager::StorageManager,
};
use iroh::protocol::Router as ARouter;
use iroh_docs::ALPN as DOCS_ALPN;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let iroh_instance = IrohMemInstance::new().await?;

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
        .accept(ALPN, access_control.clone())
        .spawn();

    access_control.upload_new("demo_vid", "funny video").await?;

    tokio::signal::ctrl_c().await?;

    Ok(())
}
