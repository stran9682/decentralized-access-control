use std::fs::{self, DirEntry};

use anyhow::Context;
use iroh::{EndpointId, endpoint::SendStream};
use iroh_blobs::HashAndFormat;
use rs_merkle::{MerkleTree, algorithms::Sha256};
use tokio::{
    fs::File,
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

        todo!("Read Merkle proof");

        tokio::io::copy(&mut recv, file_writer).await?;

        todo!("Verify Merkle proof");

        conn.close(0u32.into(), b"Successfully retrieved file.");

        Ok(true)
    }

    pub async fn send(&self, tag: &str, send: &mut SendStream) -> anyhow::Result<bool> {
        if let Some(tag) = self.iroh_instance.blobs().tags().get(tag).await? {
            send.write_all(&[Status::Allowed as u8]).await?;

            todo!("Send Merkle proof");
            
            let mut reader = self.iroh_instance.blobs().reader(tag.hash);
            tokio::io::copy(&mut reader, send).await?;

            Ok(true)
        } else {
            send.write_all(&[Status::FileNotFound as u8]).await?;
            Ok(false)
        }
    }

    pub async fn upload_dir(&self, path: &str) -> anyhow::Result<String> {

        let mut entries: Vec<DirEntry> = fs::read_dir(path)?
            .map(|file| file.map_err(anyhow::Error::from))
            .collect::<anyhow::Result<_>>()?;
        entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        let mut leaves: Vec<[u8; 32]> = Vec::new();
        let mut hash_formats: Vec<(HashAndFormat, String)> = Vec::new();

        for entry in entries {
            let file = File::open(entry.path()).await?;

            let hash = sha256::async_digest::try_async_digest(entry.path()).await?;
            let hash_bytes: [u8; 32] = hash.as_bytes().try_into()?;
            leaves.push(hash_bytes);

            let stream = ReaderStream::new(file);

            let res = self
                .iroh_instance
                .blobs()
                .add_stream(stream)
                .await
                .temp_tag()
                .await?;

            hash_formats.push((
                res.hash_and_format(),
                entry.file_name().to_string_lossy().into_owned(),
            ));
        }

        let merkle_tree = MerkleTree::<Sha256>::from_leaves(&leaves);
        let merkle_root = merkle_tree
            .root_hex()
            .context("Failed to retreive root hash")?;

        for (hash_format, filename) in hash_formats.iter() {
            self.iroh_instance
                .blobs()
                .tags()
                .set(format!("{merkle_root}/{filename}"), *hash_format)
                .await?;
        }

        let leaves_hash = self.iroh_instance.blobs()
            .add_bytes(leaves.into_flattened())
            .await?
            .hash_and_format();

        self.iroh_instance.blobs().tags().set(&merkle_root, leaves_hash).await?;

        Ok(merkle_root)
    }
}
