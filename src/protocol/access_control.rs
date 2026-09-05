use anyhow::bail;
use iroh::{
    EndpointId,
    endpoint::{RecvStream, SendStream},
    protocol::ProtocolHandler,
};
use iroh_docs::DocTicket;
use serde::{Deserialize, Serialize};
use tempfile::tempfile;
use tokio::fs::File;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{
    Status, access_list::list_manager::AccessListManager, store::storage_manager::StorageManager,
};

#[derive(Debug, Clone)]
pub struct AccessControl {
    list_manager: AccessListManager,
    storage_manager: StorageManager,
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
    pub fn new(list_manager: AccessListManager, storage_manager: StorageManager) -> Self {
        Self {
            list_manager,
            storage_manager,
        }
    }

    pub async fn make_request(
        &self,
        endpoint_id: Option<EndpointId>,
        request: &Request,
    ) -> anyhow::Result<File> {
        let mut tempfile = tokio::fs::File::from_std(tempfile()?);

        if let Some(endpoint_id) = endpoint_id {
            self.storage_manager
                .retreive_remote(endpoint_id, request, &mut tempfile)
                .await?;
        } else {
            self.storage_manager
                .retrieve_local(&request.resource, &request.filename, &mut tempfile)
                .await?;
        }

        Ok(tempfile)
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

        if let Some(peers) = doc.get_sync_peers().await?
            && request.decrement_attempts() > 0
        {
            for peer_bytes in peers {
                let peer_endpoint = EndpointId::from_bytes(&peer_bytes)?;

                let Ok(file) = self.make_request(Some(peer_endpoint), &request).await else {
                    continue;
                };

                let stream = ReaderStream::new(file);
                let mut stream = StreamReader::new(stream);

                tokio::io::copy(&mut stream, send).await?;
                return Ok(());
            }

            bail!("File not found among peers")
        } else {
            send.write_all(&[Status::FileNotFound as u8]).await?;
            bail!("No available peers to transfer")
        }
    }

    pub async fn upload_new(&self, path: &str, video_name: &str) -> anyhow::Result<()> {
        let resource = self.storage_manager.upload_dir(path, video_name).await?;
        self.list_manager.new_doc(&resource, None).await?;
        Ok(())
    }

    pub async fn import(&self, ticket: DocTicket) {
        todo!("Import the ticket, request the files to backup")
    }
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    retry_attempts: u8,
    resource: String,
    filename: String,
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
        self.retry_attempts -= 1;
        self.retry_attempts
    }
}
