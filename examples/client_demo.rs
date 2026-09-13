use std::{env, str::FromStr, time::Duration};

use anyhow::bail;
use decentralized_access_control::{
    ALPN,
    access_list::list_manager::AccessListManager,
    iroh::iroh_mem_instance::IrohMemInstance,
    protocol::access_control::{AccessControl, Request},
    store::storage_manager::StorageManager,
};
use iroh::{Endpoint, EndpointId, endpoint::presets, protocol::Router as ARouter};
use iroh_blobs::{ALPN as BLOBS_ALPN, BlobsProtocol, store::mem::MemStore};
use iroh_docs::{ALPN as DOCS_ALPN, DocTicket, protocol::Docs};
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

    let args: Vec<String> = env::args().collect();
    let doc_ticket = DocTicket::from_str(&args[3])?;

    if args.len() == 4
        && let Err(e) = access_control.import(doc_ticket).await
    {
        bail!("Error importing to access list, check error: {}", e)
    }

    let request_args: Vec<&str> = args[2].split("/").collect();
    let request = Request::new(
        request_args[0].to_string(),
        request_args[1].to_string(),
        "playlist.m3u8".to_string(),
    );

    while match access_control
        .make_request(Some(EndpointId::from_str(&args[1])?), &request)
        .await
    {
        Ok(Some(_)) => false,
        Ok(None) => {
            eprintln!("Not found inside access list");
            true
        }
        Err(e) => {
            eprintln!("Error occured during request: {e}");
            true
        }
    } {
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    println!("Successfully retrieved file");

    tokio::signal::ctrl_c().await?;

    Ok(())
}
