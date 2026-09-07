use std::collections::HashSet;
use std::str::FromStr;

use anyhow::Context;
use iroh::EndpointId;
use iroh_docs::{DocTicket, Entry, api::Doc, engine::LiveEvent, store::Query};
use tokio_stream::StreamExt;

use crate::iroh::iroh_mem_instance::IrohMemInstance;

#[derive(Debug, Clone)]
pub struct AccessListManager {
    iroh_instance: IrohMemInstance,
}

impl AccessListManager {
    pub fn new(iroh_instance: IrohMemInstance) -> Self {
        Self { iroh_instance }
    }

    pub async fn new_doc(&self, ticket: Option<String>) -> anyhow::Result<Doc> {
        let doc = match ticket {
            Some(ticket) => {
                let ticket = DocTicket::from_str(&ticket)?;
                let (doc, mut events) = self
                    .iroh_instance
                    .docs()
                    .import_and_subscribe(ticket)
                    .await?;

                while let Some(event) = events.next().await {
                    let event = event?;
                    match event {
                        LiveEvent::ContentReady { .. } => {
                            println!("Finished syncing");
                            break;
                        }
                        _ => {}
                    }
                }

                doc
            }
            None => self.iroh_instance.docs().create().await?,
        };

        Ok(doc)
    }

    pub async fn append_access_list(
        &self,
        doc: &Doc,
        resource: &str,
        endpoint_id: &EndpointId,
    ) -> anyhow::Result<bool> {
        let mut acl = self
            .query_for_tag(doc, resource)
            .await?
            .unwrap_or_else(|| HashSet::new());

        if acl.insert(*endpoint_id) {
            self.insert_bytes(&doc, resource, &acl).await?;
            return Ok(true);
        } else {
            return Ok(false);
        }
    }

    pub async fn get_access_list(
        &self,
        resource: &str,
    ) -> anyhow::Result<Option<(Doc, HashSet<EndpointId>)>> {
        let mut stream = self.iroh_instance.docs().list().await?;

        while let Some(Ok((namespace, _))) = stream.next().await {
            let doc = self
                .iroh_instance
                .docs()
                .open(namespace)
                .await?
                .with_context(|| {
                    format!(
                        "Couldn't open document. Namespace ({}), was not found",
                        namespace.fmt_short()
                    )
                })?;

            if let Some(access_list) = self.query_for_tag(&doc, resource).await? {
                println!("Access list:");

                for (i, peer) in access_list.iter().enumerate() {
                    println!("{i}: {peer}")
                }

                return Ok(Some((doc, access_list)));
            }
        }

        Ok(None)
    }

    async fn query_for_tag(
        &self,
        doc: &Doc,
        resource: &str,
    ) -> anyhow::Result<Option<HashSet<EndpointId>>> {
        let entries = doc.get_many(Query::single_latest_per_key().build()).await?;

        todo!("Find a more elegant solution");

        let mut entries: Vec<Result<Entry, anyhow::Error>> = entries.collect().await;
        let mut entries = entries.iter_mut();
        while let Some(Ok(entry)) = entries.next() {
            match self
                .iroh_instance
                .blobs()
                .get_bytes(entry.content_hash())
                .await
            {
                Ok(bytes) => {
                    let list_members: HashSet<EndpointId> = serde_json::from_slice(&bytes)?;
                    return Ok(Some(list_members));
                }
                Err(e) => {
                    eprint!("Error reading entry: {e}");
                    break;
                }
            }
        }

        Ok(None)
    }

    async fn insert_bytes(
        &self,
        doc: &Doc,
        resource: &str,
        access_list: &HashSet<EndpointId>,
    ) -> anyhow::Result<()> {
        let content = serde_json::to_vec(access_list)?;
        let resource = String::from(resource);

        doc.set_bytes(
            self.iroh_instance.docs().author_default().await?,
            resource,
            content,
        )
        .await?;

        println!("Updated list");
        Ok(())
    }
}
