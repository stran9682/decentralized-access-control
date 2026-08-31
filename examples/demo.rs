use std::path::PathBuf;

use decentralized_access_control::{
    ALPN,
    access_list::list_manager::AccessListManager,
    iroh::iroh_instance::IrohInstance,
    protocol::access_control::AccessControl,
    store::storage_manager::{self, StorageManager},
};
use iroh::{
    Endpoint,
    endpoint::presets::{self},
    protocol::Router,
};
use iroh_docs::ALPN as DOCS_ALPN;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let iroh_instance = IrohInstance::new(PathBuf::new()).await?;
    let server_endpoint = iroh_instance.endpoint();

    let list_manager = AccessListManager::new(iroh_instance.clone());
    let storage_manager = StorageManager::new(iroh_instance.clone());

    let access_control = AccessControl::new(list_manager.clone(), storage_manager);

    let router = Router::builder(iroh_instance.endpoint().clone())
        .accept(DOCS_ALPN, iroh_instance.docs().clone())
        .accept(ALPN, access_control.clone())
        .spawn();

    list_manager.new_doc("tag", None).await?;

    let endpoint = Endpoint::bind(presets::N0).await?;
    list_manager
        .append_access_list("tag", &endpoint.id())
        .await?;

    let conn = endpoint.connect(server_endpoint.addr(), ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;

    send.write_all(b"tag").await?;
    send.finish()?;

    todo!()
}
