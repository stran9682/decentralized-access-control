use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{Context, bail};
use iroh::EndpointId;
use iroh_docs::{DocTicket, api::Doc, store::Query};
use tokio_stream::StreamExt;

use crate::iroh::iroh_instance::IrohInstance;

#[derive(Debug, Clone)]
pub struct AccessListManager {
    iroh_instance: IrohInstance,
}

impl AccessListManager {
    pub fn new(iroh_instance: IrohInstance) -> Self {
        Self { iroh_instance }
    }

    pub async fn new_doc(&self, tag: &str, ticket: Option<String>) -> anyhow::Result<Doc> {
        let doc = match ticket {
            Some(ticket) => {
                let ticket = DocTicket::from_str(&ticket)?;
                self.iroh_instance.docs().import(ticket).await?
            }
            None => self.iroh_instance.docs().create().await?,
        };

        if let Some(access_list) = self.query_for_tag(&doc, tag).await?
            && access_list.contains(&self.iroh_instance.endpoint().id())
        {
            self.insert_bytes(&doc, tag, &access_list).await?;
        }

        Ok(doc)
    }

    pub async fn append_access_list(
        &self,
        resource: &str,
        endpoint_id: &EndpointId,
    ) -> anyhow::Result<bool> {
        if let Some((doc, mut access_list)) = self.get_access_list(resource, endpoint_id).await? {
            if access_list.insert(*endpoint_id) {
                self.insert_bytes(&doc, resource, &access_list).await?;
                return Ok(true);
            } else {
                return Ok(false);
            }
        }

        bail!("Access list associateed with tag not found")
    }

    pub async fn get_access_list(
        &self,
        resource: &str,
        endpoint_id: &EndpointId,
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

            if let Some(access_list) = self.query_for_tag(&doc, resource).await?
                && access_list.contains(endpoint_id)
            {
                return Ok(Some((doc, access_list)));
            }
        }

        return Ok(None);
    }

    async fn query_for_tag(
        &self,
        doc: &Doc,
        resource: &str,
    ) -> anyhow::Result<Option<HashSet<EndpointId>>> {
        if let Some(entry) = doc.get_one(Query::key_exact(resource).build()).await? {
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
                Err(e) => return Err(e.into()),
            }
        } else {
            Ok(None)
        }
    }

    async fn insert_bytes(
        &self,
        doc: &Doc,
        tag: &str,
        access_list: &HashSet<EndpointId>,
    ) -> anyhow::Result<()> {
        let content = serde_json::to_vec(access_list)?;
        let tag = String::from(tag);

        doc.set_bytes(
            self.iroh_instance.docs().author_default().await?,
            tag.clone(),
            content,
        )
        .await?;

        Ok(())
    }
}
