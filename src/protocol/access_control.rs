use anyhow::bail;
use iroh::{
    EndpointId,
    endpoint::{RecvStream, SendStream},
    protocol::ProtocolHandler,
};
use iroh_docs::{ContentStatus, DocTicket, Entry, api::Doc, engine::LiveEvent, store::Query};
use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::AsyncWriteExt};
use tokio_stream::StreamExt;
use tokio_util::io::{ReaderStream, StreamReader};

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
                if let Err(e) = access_control
                    .handle_request(peer, &mut send, &mut recv)
                    .await
                {
                    eprintln!("Error handling request: {e}")
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
    ) -> anyhow::Result<File> {
        if let Some(endpoint_id) = endpoint_id {
            println!("Making request to: {}", endpoint_id);

            match self
                .storage_manager
                .retreive_remote(endpoint_id, request)
                .await
            {
                Ok(Some(file)) => Ok(file),
                Ok(None) => bail!("Error occurred retrieving file"),
                Err(e) => bail!("Network occured retrieving file {e}"),
            }
        } else {
            return self
                .storage_manager
                .retrieve_local(&request.resource, &request.filename)
                .await;
        }
    }

    async fn handle_request(
        &self,
        endpoint_id: EndpointId,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> anyhow::Result<()> {
        let mut len_buf = [0u8; size_of::<u32>()];
        recv.read_exact(&mut len_buf).await?;
        let req_len = u32::from_be_bytes(len_buf);

        let mut request_bytes = vec![0u8; req_len as usize];
        recv.read_exact(&mut request_bytes).await?;
        let mut request: Request = serde_json::from_slice(&request_bytes)?;

        let Some((doc, access_list)) = self.list_manager.get_access_list(&request.resource).await?
        else {
            send.write_all(&[Status::ResourceNotFound as u8]).await?;
            bail!("Requested access list not found")
        };

        if !access_list.contains(&endpoint_id) {
            send.write_all(&[Status::Denied as u8]).await?;
            bail!("EndpointId not found inside access list.")
        }

        // If the file is available locally, send it to the requester
        // when it isn't, check if anyone else has it
        if self
            .storage_manager
            .send(&request.resource, &request.filename, send)
            .await?
        {
            return Ok(());
        }

        todo!("Fix up getting file from another peer");

        // if let Some(peers) = doc.get_sync_peers().await?
        //     && request.retry_attempts > 0
        // {
        //     request.decrement_attempts();
        //     for peer_bytes in peers {
        //         let peer_endpoint = EndpointId::from_bytes(&peer_bytes)?;

        //         if self.forward_request(peer_endpoint, &request, send).await? {
        //             return Ok(());
        //         }
        //     }

        //     bail!("File not found among peers")
        // } else {
        //     send.write_all(&[Status::FileNotFound as u8]).await?;
        //     bail!("No available peers to transfer")
        // }
    }

    async fn forward_request(
        &self,
        endpoint_id: EndpointId,
        request: &Request,
        send: &mut SendStream,
    ) -> anyhow::Result<bool> {
        let conn = self.endpoint_id;
        let endpoint = self.storage_manager.endpoint();
        let connection = endpoint.connect(endpoint_id, crate::ALPN).await?;
        let (mut upstream_send, mut upstream_recv) = connection.open_bi().await?;

        let request_bytes = serde_json::to_vec(request)?;
        upstream_send.write_u32(request_bytes.len() as u32).await?;
        upstream_send.write_all(&request_bytes).await?;

        let mut status = [0u8; 1];
        upstream_recv.read_exact(&mut status).await?;
        if status[0] != Status::Allowed as u8 {
            return Ok(false);
        }

        send.write_all(&status).await?;
        tokio::io::copy(&mut upstream_recv, send).await?;
        let _ = conn;
        Ok(true)
    }

    pub async fn upload_new(&self, path: &str, video_name: &str) -> anyhow::Result<Doc> {
        let resource = self.storage_manager.upload_dir(path, video_name).await?;
        let doc = self.list_manager.new_doc(None).await?;
        self.list_manager
            .append_access_list(&doc, &resource, &self.endpoint_id)
            .await?;

        let ticket = doc
            .share(
                iroh_docs::api::protocol::ShareMode::Write,
                Default::default(),
            )
            .await?;

        println!("Ticket: {}", ticket);
        println!("Resource: {}", resource);

        Ok(doc)
    }

    pub async fn import(&self, ticket: DocTicket) -> anyhow::Result<()> {
        println!("Importing ticket: {}", ticket);
        let doc = self.list_manager.new_doc(Some(ticket.to_string())).await?;
        let mut events = doc.subscribe().await?;

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

        let entries = doc.get_many(Query::single_latest_per_key()).await?;
        let mut entries: Vec<Result<Entry, anyhow::Error>> = entries.collect().await;
        let mut entries = entries.iter_mut();
        while let Some(Ok(entry)) = entries.next() {
            let resource = String::from_utf8(entry.key().to_vec())?;

            println!("Appending to: {}", resource);

            self.list_manager
                .append_access_list(&doc, &resource, &self.endpoint_id)
                .await?;
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    retry_attempts: u8,
    pub resource: String,
    pub filename: String,
}

impl Request {
    pub fn new(retry_attempts: u8, resource: String, filename: String) -> Self {
        Request {
            retry_attempts,
            resource,
            filename,
        }
    }

    pub fn decrement_attempts(&mut self) -> u8 {
        self.retry_attempts = self.retry_attempts.saturating_sub(1);
        self.retry_attempts
    }
}
