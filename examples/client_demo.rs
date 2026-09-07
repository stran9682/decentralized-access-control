use std::{env, str::FromStr, time::Duration};

use anyhow::bail;
use decentralized_access_control::{
    ALPN,
    access_list::list_manager::AccessListManager,
    iroh::iroh_mem_instance::IrohMemInstance,
    protocol::access_control::{AccessControl, Request},
    store::storage_manager::StorageManager,
};
use iroh::{EndpointId, endpoint::presets, protocol::Router as ARouter};
use iroh_blobs::{ALPN as BLOBS_ALPN, BlobsProtocol, store::mem::MemStore};
use iroh_docs::{ALPN as DOCS_ALPN, DocTicket, protocol::Docs};
use iroh_gossip::{ALPN as GOSSIP_ALPN, Gossip};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = iroh::Endpoint::bind(presets::N0).await?;
    let blobs = MemStore::new();
    let gossip = Gossip::builder().spawn(endpoint.clone());

    let docs = Docs::memory()
        .spawn(endpoint.clone(), (*blobs).clone(), gossip.clone())
        .await?;

    let iroh_instance = IrohMemInstance::new(blobs.clone(), docs, endpoint);

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
        .accept(GOSSIP_ALPN, gossip)
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blobs, None))
        .spawn();

    let args: Vec<String> = env::args().collect();
    let doc_ticket = DocTicket::from_str(&args[3])?;

    if args.len() == 4
        && let Err(e) = access_control.import(doc_ticket).await
    {
        bail!("Error importing to access list, check error: {}", e)
    }

    let request = Request::new(1, args[2].clone(), "playlist.m3u8".to_string());

    while let Err(e) = access_control
        .make_request(Some(EndpointId::from_str(&args[1])?), &request)
        .await
    {
        eprintln!("Error retreiving file: {}", e);
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    println!("Successfully retrieved file");

    tokio::signal::ctrl_c().await?;

    Ok(())
}
