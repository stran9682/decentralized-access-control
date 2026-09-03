use anyhow::{Context, bail};
use iroh::{EndpointId, endpoint::SendStream};
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};
use tokio_util::io::ReaderStream;

use crate::{ALPN, Status, iroh::iroh_instance::IrohInstance, protocol::access_control::Request};

#[derive(Debug, Clone)]
pub struct StorageManager {
    iroh_instance: IrohInstance,
}

impl StorageManager {
    pub fn new(iroh_instance: IrohInstance) -> Self {
        Self { iroh_instance }
    }

    pub async fn retrieve_local(
        &self,
        resource: &str,
        filename: &str,
        file_writer: &mut File,
    ) -> anyhow::Result<()> {
        let tag = format!("{resource}/{filename}");

        let tag_info = self
            .iroh_instance
            .blobs()
            .tags()
            .get(tag)
            .await?
            .context("Tag not found locally")?;
        let mut reader = self.iroh_instance.blobs().reader(tag_info.hash);
        tokio::io::copy(&mut reader, file_writer).await?;

        Ok(())
    }

    pub async fn retreive_remote(
        &self,
        endpoint_id: EndpointId,
        request: &Request,
        file_writer: &mut File,
    ) -> anyhow::Result<bool> {
        let endpoint = self.iroh_instance.endpoint();

        let conn = endpoint.connect(endpoint_id, ALPN).await?;

        let (mut send, mut recv) = conn.open_bi().await?;

        let request_bytes = serde_json::to_vec(request)?;
        let bytes_len = request_bytes.len() as u32;

        send.write_u32(bytes_len).await?;
        send.write_all(&request_bytes).await?;

        let mut status_buf = [0u8; 1];
        recv.read_exact(&mut status_buf).await?;

        if status_buf[0] != (Status::Allowed as u8) {
            return Ok(false);
        }

        tokio::io::copy(&mut recv, file_writer).await?;

        conn.close(0u32.into(), b"Successfully retrieved file.");

        Ok(true)
    }

    pub async fn send(&self, tag: &str, send: &mut SendStream) -> anyhow::Result<bool> {
        if let Some(tag) = self.iroh_instance.blobs().tags().get(tag).await? {
            send.write_all(&[Status::Allowed as u8]).await?;
            let mut reader = self.iroh_instance.blobs().reader(tag.hash);
            tokio::io::copy(&mut reader, send).await?;

            Ok(true)
        } else {
            send.write_all(&[Status::FileNotFound as u8]).await?;
            Ok(false)
        }
    }

    pub async fn upload_dir(&self, path: &str) -> anyhow::Result<()> {
        let mut entries = fs::read_dir(path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let file = File::open(entry.path()).await?;
            // let hash = sha256::digest();

            let stream = ReaderStream::new(file);

            let store = self.iroh_instance.blobs().clone();

            tokio::spawn(async move {
                if let Err(e) = store
                    .add_stream(stream)
                    .await
                    .with_named_tag(format!(
                        "{}/{}",
                        todo!("Merkle Tree root hash"),
                        entry.file_name().to_string_lossy()
                    ))
                    .await
                {
                    eprintln!("Failed to add to store: {}", e)
                }
            });
        }

        Ok(())
    }
}
