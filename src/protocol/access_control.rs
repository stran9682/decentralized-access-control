use iroh::{
    EndpointId,
    endpoint::{RecvStream, SendStream},
    protocol::ProtocolHandler,
};

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

    async fn handle_request(
        &self,
        endpoint_id: EndpointId,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> anyhow::Result<()> {
        let bytes = recv.read_to_end(256).await?;
        let tag = String::from_utf8(bytes)?;

        if self
            .list_manager
            .get_access_list(&tag, &endpoint_id)
            .await?
            .is_none()
        {
            send.finish()?;
            return Ok(());
        }

        self.storage_manager.retrieve(&tag, send).await?;
        send.finish()?;

        Ok(())
    }
}
