use std::collections::HashSet;
use std::str::FromStr;

use iroh::EndpointId;
use iroh_docs::{DocTicket, NamespaceId, api::Doc, engine::LiveEvent, store::Query};
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
                    if let Ok(LiveEvent::ContentReady { .. }) = event {
                        println!("Finished syncing");
                        break;
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
        resource: Option<&str>,
        endpoint_id: &EndpointId,
    ) -> anyhow::Result<bool> {
        let namespace = doc.id().to_string();

        let mut acl = self
            .query_for_tag(doc, &namespace, resource)
            .await?
            .unwrap_or_else(HashSet::new);

        if acl.insert(*endpoint_id) {
            self.insert_bytes(doc, &namespace, resource, &acl).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn get_doc(&self, namespace: &str) -> anyhow::Result<Option<Doc>> {
        let namespace = NamespaceId::from_str(namespace)?;

        let doc = self.iroh_instance.docs().open(namespace).await?;

        Ok(doc)
    }

    pub async fn get_access_list(
        &self,
        namespace: &str,
        resource: &str,
    ) -> anyhow::Result<Option<(Doc, HashSet<EndpointId>)>> {
        let Some(doc) = self.get_doc(namespace).await? else {
            return Ok(None);
        };

        for resource_tag in [None, Some(resource)] {
            if let Some(access_list) = self.query_for_tag(&doc, namespace, resource_tag).await? {
                println!("Access list:");
                for (i, peer) in access_list.iter().enumerate() {
                    println!("{i}: {peer}");
                }
                return Ok(Some((doc, access_list)));
            }
        }

        Ok(None)
    }

    async fn query_for_tag(
        &self,
        doc: &Doc,
        namespace: &str,
        resource: Option<&str>,
    ) -> anyhow::Result<Option<HashSet<EndpointId>>> {
        let mut tag = namespace.to_string();
        if let Some(resource) = resource {
            tag.push_str(&format!("/{resource}"));
        };

        let Some(entry) = doc
            .get_one(Query::single_latest_per_key().key_exact(tag).build())
            .await?
        else {
            return Ok(None);
        };

        match self
            .iroh_instance
            .blobs()
            .get_bytes(entry.content_hash())
            .await
        {
            Ok(bytes) => {
                let list_members: HashSet<EndpointId> = serde_json::from_slice(&bytes)?;
                Ok(Some(list_members))
            }
            Err(e) => {
                eprintln!("Error reading entry: {e}");
                Ok(None)
            }
        }
    }

    async fn insert_bytes(
        &self,
        doc: &Doc,
        namespace: &str,
        resource: Option<&str>,
        access_list: &HashSet<EndpointId>,
    ) -> anyhow::Result<()> {
        let mut tag = namespace.to_string();
        if let Some(resource) = resource {
            tag.push_str(&format!("/{resource}"));
        };

        let content = serde_json::to_vec(access_list)?;

        doc.set_bytes(
            self.iroh_instance.docs().author_default().await?,
            tag,
            content,
        )
        .await?;

        println!("Updated list");
        Ok(())
    }
}
