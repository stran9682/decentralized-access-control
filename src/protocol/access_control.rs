use anyhow::bail;
use iroh::{
    EndpointId,
    endpoint::{RecvStream, SendStream},
    protocol::ProtocolHandler,
};
use iroh_docs::{DocTicket, api::Doc};
use serde::{Deserialize, Serialize};
use tokio::fs::File;

use crate::{
    Status, access_list::list_manager::AccessListManager, store::storage_manager::StorageManager,
};

#[derive(Debug, Clone)]
pub struct AccessControl {
    list_manager: AccessListManager,
    storage_manager: StorageManager,
    endpoint_id: EndpointId,
}

impl ProtocolHandler for AccessControl {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let peer: EndpointId = connection.remote_id();
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let access_control = self.clone();

            tokio::spawn(async move {
                match access_control
                    .handle_request(peer, &mut send, &mut recv)
                    .await
                {
                    Err(e) => eprintln!("Error handling request: {e}"),
                    Ok(false) => eprintln!("Invalid data"),
                    _ => (),
                }

                if let Err(e) = send.finish() {
                    eprintln!("Stream was closed already: {e}")
                }
            });
        }

        Ok(())
    }
}

impl AccessControl {
    pub fn new(
        list_manager: AccessListManager,
        storage_manager: StorageManager,
        endpoint_id: EndpointId,
    ) -> Self {
        Self {
            list_manager,
            storage_manager,
            endpoint_id,
        }
    }

    pub async fn make_request(
        &self,
        endpoint_id: Option<EndpointId>,
        request: &Request,
    ) -> anyhow::Result<Option<File>> {
        if let Some(endpoint_id) = endpoint_id {
            println!("Making request to: {}", endpoint_id);

            self.storage_manager
                .retreive_remote(endpoint_id, request)
                .await
        } else {
            self.storage_manager
                .retrieve_local(&request.resource, &request.filename)
                .await
                .map(Some)
        }
    }

    async fn handle_request(
        &self,
        endpoint_id: EndpointId,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> anyhow::Result<bool> {
        let mut len_buf = [0u8; size_of::<u32>()];
        recv.read_exact(&mut len_buf).await?;
        let req_len = u32::from_be_bytes(len_buf);

        let mut request_bytes = vec![0u8; req_len as usize];
        recv.read_exact(&mut request_bytes).await?;
        let request: Request = serde_json::from_slice(&request_bytes)?;

        let Some((_, access_list)) = self
            .list_manager
            .get_access_list(&request.namespace, &request.resource)
            .await?
        else {
            eprintln!("Requested access list not found");
            send.write_all(&[Status::ResourceNotFound as u8]).await?;
            return Ok(false);
        };

        if !access_list.contains(&endpoint_id) {
            eprintln!("EndpointId not found inside access list.");
            send.write_all(&[Status::Denied as u8]).await?;
            return Ok(false);
        }

        self.storage_manager
            .send(
                &request.namespace,
                &request.resource,
                &request.filename,
                send,
            )
            .await?;

        Ok(true)
    }

    pub async fn upload_new(
        &self,
        path: &str,
        video_name: &str,
        namespace: Option<&str>,
    ) -> anyhow::Result<Doc> {
        let (namespace, doc) = if let Some(namespace) = namespace {
            if let Some(doc) = self.list_manager.get_doc(namespace).await? {
                (namespace.to_string(), doc)
            } else {
                bail!("Document not found")
            }
        } else {
            let doc = self.list_manager.new_doc(None).await?;
            (doc.id().into_public_key()?.to_string(), doc)
        };

        let namespace_resource = self
            .storage_manager
            .upload_dir(path, video_name, &namespace)
            .await?;

        self.list_manager
            .append_access_list(&doc, None, &self.endpoint_id)
            .await?;

        let ticket = doc
            .share(
                iroh_docs::api::protocol::ShareMode::Write,
                Default::default(),
            )
            .await?;

        println!("Resource: {}", namespace_resource);
        println!("Ticket: {}", ticket);

        Ok(doc)
    }

    pub async fn import(&self, ticket: DocTicket) -> anyhow::Result<()> {
        println!("Importing ticket: {}", ticket);
        let doc = self.list_manager.new_doc(Some(ticket.to_string())).await?;

        self.list_manager
            .append_access_list(&doc, None, &self.endpoint_id)
            .await?;

        // TODO : sync blobs

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    pub namespace: String,
    pub resource: String,
    pub filename: String,
}

impl Request {
    pub fn new(namespace: String, resource: String, filename: String) -> Self {
        Request {
            namespace,
            resource,
            filename,
        }
    }
}
