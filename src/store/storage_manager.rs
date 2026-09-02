use anyhow::bail;
use iroh::{EndpointId, endpoint::SendStream};
use tokio::fs::{self, File};
use tokio_util::io::ReaderStream;

use crate::{ALPN, iroh::iroh_instance::IrohInstance, protocol::access_control::Status};

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

        if let Some(tag) = self.iroh_instance.blobs().tags().get(tag).await? {
            let mut reader = self.iroh_instance.blobs().reader(tag.hash);
            tokio::io::copy(&mut reader, file_writer).await?;
            Ok(())
        } else {
            bail!("Tag not found locally");
        }
    }

    pub async fn retreive_remote(
        &self,
        endpoint_id: EndpointId,
        resource: &str,
        filename: &str,
        file_writer: &mut File,
    ) -> anyhow::Result<()> {
        let endpoint = self.iroh_instance.endpoint();

        let conn = endpoint.connect(endpoint_id, ALPN).await?;

        let (mut send, mut recv) = conn.open_bi().await?;

        let request = format!("{}/{}", resource, filename);

        send.write_all(request.as_bytes()).await?;

        let mut status_buf = [0u8; 1];
        recv.read_exact(&mut status_buf).await?;

        if status_buf[0] == (Status::Denied as u8) {
            bail!("Request failed: Accesss was denied")
        }

        tokio::io::copy(&mut recv, file_writer).await?;

        conn.close(0u32.into(), b"Successfully retrieved file.");

        Ok(())
    }

    pub async fn send(&self, tag: &str, send: &mut SendStream) -> anyhow::Result<()> {
        if let Some(tag) = self.iroh_instance.blobs().tags().get(tag).await? {
            send.write_all(&[Status::Allowed as u8]).await?;
            let mut reader = self.iroh_instance.blobs().reader(tag.hash);
            tokio::io::copy(&mut reader, send).await?;
            send.finish()?;

            Ok(())
        } else {
            send.write_all(&[Status::Denied as u8]).await?;
            send.finish()?;
            bail!("Tag not found")
        }
    }

    pub async fn upload_dir(&self, path: &str, resource: &str) -> anyhow::Result<()> {
        let mut entries = fs::read_dir(path).await?;

        todo!("Hash each file and use root hash as the resource name");

        while let Some(entry) = entries.next_entry().await? {
            let file = File::open(entry.path()).await?;
            let stream = ReaderStream::new(file);

            let store = self.iroh_instance.blobs().clone();
            let resource = String::from(resource);

            tokio::spawn(async move {
                if let Err(e) = store
                    .add_stream(stream)
                    .await
                    .with_named_tag(format!(
                        "{}/{}",
                        resource,
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
