use anyhow::bail;
use iroh::{
    endpoint::{Connection, VarInt},
    protocol::ProtocolHandler,
};
use tokio::io::AsyncReadExt;

use crate::{access_list::list_manager::AccessListManager, Status};

#[derive(Debug)]
pub struct VideoDiscovery {
    list_manager: AccessListManager,
}

impl ProtocolHandler for VideoDiscovery {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        if let Err(e) = self.handle_request(&connection).await {
            eprintln!("Error occured handling video discovery request:  {}", e);
            connection.close(VarInt::from_u32(1), e.to_string().as_bytes());
        }
        connection.close(VarInt::from_u32(1), b"successfully retrieved videos");

        Ok(())
    }
}

impl VideoDiscovery {
    pub fn new(list_manager: AccessListManager) -> Self {
        Self { list_manager }
    }

    async fn handle_request(&self, connection: &Connection) -> anyhow::Result<()> {
        let (mut send, mut recv) = connection.accept_bi().await?;

        let len = recv.read_u32().await?;

        let mut request_bytes: Vec<u8> = vec![0u8; len as usize];
        recv.read_exact(&mut request_bytes).await?;

        let namespace = String::from_utf8(request_bytes)?;

        let Some(list) = self
            .list_manager
            .get_authorized_videos(&namespace, &connection.remote_id())
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
