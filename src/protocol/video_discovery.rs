use anyhow::bail;
use iroh::{
    EndpointId,
    endpoint::{RecvStream, SendStream},
    protocol::ProtocolHandler,
};
use tokio::io::AsyncReadExt;

use crate::{Status, access_list::list_manager::AccessListManager};

#[derive(Debug, Clone)]
pub struct VideoDiscovery {
    list_manager: AccessListManager,
}

impl ProtocolHandler for VideoDiscovery {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        println!("recieved request");

        let peer: EndpointId = connection.remote_id();
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let discovery = self.clone();

            tokio::spawn(async move {
                if let Err(e) = discovery.handle_request(&mut send, &mut recv, peer).await {
                    eprintln!("Error occured handling discovery request, {}", e);
                }
            });
        }

        println!("Finished request");

        Ok(())
    }
}

impl VideoDiscovery {
    pub fn new(list_manager: AccessListManager) -> Self {
        Self { list_manager }
    }

    async fn handle_request(
        &self,
        send: &mut SendStream,
        recv: &mut RecvStream,
        endpoint_id: EndpointId,
    ) -> anyhow::Result<()> {
        let len = recv.read_u32().await?;

        let mut request_bytes: Vec<u8> = vec![0u8; len as usize];
        recv.read_exact(&mut request_bytes).await?;

        let namespace = String::from_utf8(request_bytes)?;

        let Some(list) = self
            .list_manager
            .get_authorized_videos(&namespace, &endpoint_id)
            .await?
        else {
            send.write(&[Status::FileNotFound as u8]).await?;
            send.finish()?;
            bail!("Couldn't find namespace");
        };

        send.write(&[Status::Allowed as u8]).await?;

        let list_bytes = serde_json::to_vec(&list)?;
        send.write_all(&list_bytes).await?;
        send.finish()?;

        Ok(())
    }
}
