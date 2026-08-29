use iroh::{endpoint::presets, protocol::Router, Endpoint};
use iroh_blobs::{store::mem::MemStore, BlobsProtocol, ALPN as BLOBS_ALPN};
use iroh_docs::{protocol::Docs, ALPN as DOCS_ALPN};
use iroh_gossip::{net::Gossip, ALPN as GOSSIP_ALPN};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;

    let blobs = MemStore::default();

    let gossip = Gossip::builder().spawn(endpoint.clone());

    let docs = Docs::memory()
        .spawn(endpoint.clone(), (*blobs).clone(), gossip.clone())
        .await?;

    let router = Router::builder(endpoint.clone())
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blobs, None))
        .accept(GOSSIP_ALPN, gossip)
        .accept(DOCS_ALPN, docs.clone())
        .spawn();

    
        

    Ok(())
}
