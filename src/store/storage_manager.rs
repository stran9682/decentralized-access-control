use iroh::endpoint::SendStream;
use tokio::fs::{self, File};
use tokio_util::io::ReaderStream;

use crate::iroh::iroh_instance::IrohInstance;

#[derive(Debug, Clone)]
pub struct StorageManager {
    iroh_instance: IrohInstance,
}

impl StorageManager {
    pub fn new(iroh_instance: IrohInstance) -> Self {
        Self { iroh_instance }
    }

    pub async fn retrieve(&self, tag: &str, send: &mut SendStream) -> anyhow::Result<()> {
        if let Some(tag) = self.iroh_instance.blobs().tags().get(tag).await? {
            let mut reader = self.iroh_instance.blobs().reader(tag.hash);
            tokio::io::copy(&mut reader, send).await?;
        } else {
            todo!("Send back error")
        }

        send.finish()?;
        Ok(())
    }

    pub async fn upload_dir(&self, path: &str, tag: &str) -> anyhow::Result<()> {
        let mut entries = fs::read_dir(path).await?;

        while let Some(entry) = entries.next_entry().await? {
            let file = File::open(entry.path()).await?;
            let stream = ReaderStream::new(file);

            let store = self.iroh_instance.blobs().clone();
            let tag = String::from(tag);

            tokio::spawn(async move {
                if let Err(e) = store
                    .add_stream(stream)
                    .await
                    .with_named_tag(format!("{}/{}", tag, entry.file_name().to_string_lossy()))
                    .await
                {
                    eprintln!("Failed to add to store: {}", e)
                }
            });
        }

        Ok(())
    }
}
