use std::{env, str::FromStr, time::Duration};

use iroh::{Endpoint, endpoint::presets, protocol::Router};
use iroh_blobs::{ALPN as BLOBS_ALPN, BlobsProtocol, store::mem::MemStore};
use iroh_docs::{
    ALPN as DOCS_ALPN, ContentStatus, DocTicket, api::protocol::ShareMode, engine::LiveEvent,
    protocol::Docs,
};
use iroh_gossip::{ALPN as GOSSIP_ALPN, Gossip};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    let blobs = MemStore::new();
    let gossip = Gossip::builder().spawn(endpoint.clone());

    let docs = Docs::memory()
        .spawn(endpoint.clone(), (*blobs).clone(), gossip.clone())
        .await?;

    let _router = Router::builder(endpoint)
        .accept(DOCS_ALPN, docs.clone())
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blobs, None))
        .accept(GOSSIP_ALPN, gossip)
        .spawn();

    let args: Vec<String> = env::args().collect();
    let doc = if args.len() == 2 {
        let doc = docs.import(DocTicket::from_str(&args[1])?).await?;
        doc
    } else {
        let doc = docs.create().await?;
        let ticket = doc.share(ShareMode::Write, Default::default()).await?;

        println!("Ticket: {}", ticket);
        doc.set_bytes(
            docs.author_default().await?,
            "counter",
            0u64.to_be_bytes().to_vec(),
        )
        .await?;
        doc
    };

    let mut events = doc.subscribe().await?;
    while let Some(event) = events.next().await {
        let event = event?;
        match event {
            LiveEvent::InsertRemote {
                content_status: ContentStatus::Complete,
                ..
            }
            | LiveEvent::InsertLocal { .. } => {
                println!("Received update")
            }
            LiveEvent::ContentReady { hash } => {
                let blob_bytes = blobs.get_bytes(hash).await?;
                let bytes: [u8; 8] = blob_bytes
                    .as_ref()
                    .try_into()
                    .expect("Slice must be exactly 8 bytes long");

                let mut counter: u64 = u64::from_be_bytes(bytes);
                counter = counter + 1;

                println!("Counter: {}", counter);

                doc.set_bytes(
                    docs.author_default().await?,
                    "counter",
                    counter.to_be_bytes().to_vec(),
                )
                .await?;

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            _ => {}
        }
    }

    Ok(())
}
