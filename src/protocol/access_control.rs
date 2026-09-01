use anyhow::{Context, bail};
use iroh::{
    EndpointId,
    endpoint::{RecvStream, SendStream},
    protocol::ProtocolHandler,
};
use iroh_docs::DocTicket;
use tempfile::tempfile;
use tokio::fs::File;

use crate::{access_list::list_manager::AccessListManager, store::storage_manager::StorageManager};

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
                    eprintln!("Error handling request: {}", e)
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
        resource: &str,
        filename: &str,
    ) -> anyhow::Result<File> {
        let mut tempfile = tokio::fs::File::from_std(tempfile()?);

        if let Some(endpoint_id) = endpoint_id {
            self.storage_manager
                .retreive_remote(endpoint_id, resource, filename, &mut tempfile)
                .await?;
        } else {
            self.storage_manager
                .retrieve_local(resource, filename, &mut tempfile)
                .await;
        }

        Ok(tempfile)
    }

    async fn handle_request(
        &self,
        endpoint_id: EndpointId,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> anyhow::Result<()> {
        let bytes = recv.read_to_end(256).await?;
        let tag = String::from_utf8(bytes)?;

        let resource = tag.split('/').next().context("Resource Invalid format")?;

        if self
            .list_manager
            .get_access_list(&resource, &endpoint_id)
            .await?
            .is_none()
        {
            send.write_all(&[Status::Denied as u8]).await?;
            send.finish()?;

            bail!("EndpointId not found inside access list.")
        }

        self.storage_manager.send(&tag, send).await?;

        Ok(())
    }

    pub async fn upload_new(&self, tag: &str, path: &str) {
        todo!()
    }

    pub async fn import(&self, ticket: DocTicket) {
        todo!()
    }
}

#[repr(u8)]
pub enum Status {
    Denied = 0x00,
    Allowed = 0x01,
}
