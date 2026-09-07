use std::{env, str::FromStr};

use anyhow::bail;
use decentralized_access_control::{
    ALPN,
    access_list::list_manager::AccessListManager,
    iroh::iroh_mem_instance::IrohMemInstance,
    protocol::access_control::{AccessControl, Request},
    store::storage_manager::StorageManager,
};
use iroh::{EndpointId, protocol::Router as ARouter};
use iroh_docs::{ALPN as DOCS_ALPN, DocTicket};

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

    let _router = ARouter::builder(iroh_instance.endpoint().clone())
        .accept(DOCS_ALPN, iroh_instance.docs().clone())
        .accept(ALPN, access_control.clone())
        .spawn();

    let args: Vec<String> = env::args().collect();
    let doc_ticket = DocTicket::from_str(&args[3])?;

    if args.len() == 4
        && let Err(e) = access_control.import(doc_ticket).await
    {
        bail!("Error importing to access list, check error: {}", e)
    }

    let request = Request::new(1, args[2].clone(), "playlist.m3u8".to_string());

    println!("Making a request");
    access_control
        .make_request(Some(EndpointId::from_str(&args[1])?), &request)
        .await?;

    tokio::signal::ctrl_c().await?;

    Ok(())
}
