use std::{
    collections::HashMap,
    fs::{self, DirEntry},
};

use anyhow::{Context, bail};
use iroh::{EndpointId, endpoint::SendStream};
use iroh_blobs::HashAndFormat;
use rs_merkle::{MerkleProof, MerkleTree, algorithms::Sha256};
use serde::{Deserialize, Serialize};
use tempfile::tempfile;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tokio_util::io::ReaderStream;

use crate::{
    ALPN, Status,
    iroh::{iroh_instance::IrohInstance, iroh_mem_instance::IrohMemInstance},
    protocol::access_control::Request,
};

#[derive(Debug, Clone)]
pub struct StorageManager {
    iroh_instance: IrohMemInstance,
}

impl StorageManager {
    pub fn new(iroh_instance: IrohMemInstance) -> Self {
        Self { iroh_instance }
    }

    pub fn endpoint(&self) -> &iroh::Endpoint {
        self.iroh_instance.endpoint()
    }

    pub async fn retrieve_local(&self, resource: &str, filename: &str) -> anyhow::Result<File> {
        let mut file_writer = tokio::fs::File::from_std(tempfile()?);

        let tag = format!("{resource}/{filename}");

        let tag_info = self
            .iroh_instance
            .blobs()
            .tags()
            .get(tag)
            .await?
            .context("Tag not found locally")?;
        let mut reader = self.iroh_instance.blobs().reader(tag_info.hash);
        tokio::io::copy(&mut reader, &mut file_writer).await?;

        Ok(file_writer)
    }

    pub async fn retreive_remote(
        &self,
        endpoint_id: EndpointId,
        request: &Request,
    ) -> anyhow::Result<Option<File>> {
        let mut file_writer = tokio::fs::File::from_std(tempfile()?);

        let endpoint = self.iroh_instance.endpoint();

        let conn = endpoint.connect(endpoint_id, ALPN).await?;

        let (mut send, mut recv) = conn.open_bi().await?;

        let request_bytes = serde_json::to_vec(request)?;
        let request_len = request_bytes.len() as u32;

        send.write_u32(request_len).await?;
        send.write_all(&request_bytes).await?;

        let mut status_buf = [0u8; 1];
        recv.read_exact(&mut status_buf).await?;

        if status_buf[0] != (Status::Allowed as u8) {
            return Ok(None);
        }

        let proof_len = recv.read_u32().await?;
        let mut proof_buf = vec![0u8; proof_len as usize];
        recv.read_exact(&mut proof_buf).await?;

        let proof: MerkleVerification = serde_json::from_slice(&proof_buf)?;
        let merkle_proof = MerkleProof::<Sha256>::from_bytes(&proof.merkle_proof)?;

        let merkle_root: [u8; 32] = hex::decode(&request.resource)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid Merkle root length"))?;

        let file_bytes = recv.read_to_end(usize::MAX).await?;
        file_writer.write_all(&file_bytes).await?;

        let hash = sha256::digest(&file_bytes);
        let hash_bytes: [u8; 32] = hex::decode(&hash)?
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid hash length"))?;

        if !merkle_proof.verify(merkle_root, &[proof.index], &[hash_bytes], proof.count) {
            return Ok(None);
        }

        conn.close(0u32.into(), b"Successfully retrieved file.");

        Ok(Some(file_writer))
    }

    pub async fn send(
        &self,
        resource: &str,
        filename: &str,
        send: &mut SendStream,
    ) -> anyhow::Result<bool> {
        let Some(proof_tag) = self.iroh_instance.blobs().tags().get(resource).await? else {
            eprintln!("Tag not found");
            send.write_all(&[Status::ResourceNotFound as u8]).await?;
            bail!("Tag not found")
        };

        let tag = format!("{resource}/{filename}");
        let Some(file_tag) = self.iroh_instance.blobs().tags().get(tag).await? else {
            println!("File not found");
            // send.write_all(&[Status::FileNotFound as u8]).await?;
            return Ok(false);
        };

        send.write_all(&[Status::Allowed as u8]).await?;
        println!("Accepted");

        let metadata_bytes = self.iroh_instance.blobs().get_bytes(proof_tag.hash).await?;
        let metadata: VideoMetadata = serde_json::from_slice(&metadata_bytes)?;

        let proof = metadata
            .generate_proof(filename)
            .context("Couldn't generate proof")?;

        let proof_bytes = serde_json::to_vec(&proof)?;

        send.write_u32(proof_bytes.len() as u32).await?;
        send.write_all(&proof_bytes).await?;

        println!("Sending file");
        let mut reader = self.iroh_instance.blobs().reader(file_tag.hash);
        tokio::io::copy(&mut reader, send).await?;

        Ok(true)
    }

    pub async fn upload_dir(&self, path: &str, video_name: &str) -> anyhow::Result<String> {
        let mut entries: Vec<DirEntry> = fs::read_dir(path)?
            .map(|file| file.map_err(anyhow::Error::from))
            .collect::<anyhow::Result<_>>()?;
        entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        let mut hash_formats: Vec<(HashAndFormat, String, [u8; 32])> = Vec::new();
        let blobs = self.iroh_instance.blobs();

        for entry in entries {
            let file = File::open(entry.path()).await?;

            let hash = sha256::async_digest::try_async_digest(entry.path()).await?;
            let hash_bytes: [u8; 32] = hex::decode(&hash)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid hash length"))?;

            let stream = ReaderStream::new(file);

            let res = blobs.add_stream(stream).await.temp_tag().await?;

            hash_formats.push((
                res.hash_and_format(),
                entry.file_name().to_string_lossy().into_owned(),
                hash_bytes,
            ));
        }

        let merkle_tree = MerkleTree::<Sha256>::from_leaves(
            &hash_formats.iter().map(|x| x.2).collect::<Vec<_>>(),
        );
        let merkle_root = merkle_tree
            .root_hex()
            .context("Failed to retreive root hash")?;

        for (hash_format, filename, _) in hash_formats.iter() {
            self.set_tag(&merkle_root, Some(filename), *hash_format)
                .await?;
        }

        let metadata = VideoMetadata::new(
            hash_formats.into_iter().map(|h| (h.1, h.2)).collect(),
            video_name,
        );

        let leaves_hash = blobs
            .add_slice(&serde_json::to_vec(&metadata)?)
            .temp_tag()
            .await?
            .hash_and_format();

        self.set_tag(&merkle_root, None, leaves_hash).await?;

        Ok(merkle_root)
    }

    async fn set_tag(
        &self,
        resource: &str,
        filename: Option<&str>,
        value: impl Into<HashAndFormat>,
    ) -> anyhow::Result<()> {
        let tags_api = self.iroh_instance.blobs().tags();

        let tag = match filename {
            Some(filename) => format!("{resource}/{filename}"),
            None => resource.to_owned(),
        };

        if let Err(e) = tags_api.set(tag, value).await {
            match tags_api.delete_prefix(resource).await {
                Ok(num_removed) => {
                    bail!("Failed to set tag, removed {num_removed} in clean up. err: {e}")
                }
                Err(delete_err) => {
                    bail!("Failed to clean up tags: {delete_err} after failing to set tag: {e}")
                }
            }
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct VideoMetadata {
    clip_hashes: HashMap<String, (usize, [u8; 32])>,
    video_name: String,
}

impl VideoMetadata {
    pub fn new(leaves: Vec<(String, [u8; 32])>, video_name: &str) -> Self {
        let mut clip_hashes: HashMap<String, (usize, [u8; 32])> = HashMap::new();

        for (index, (filename, leaf)) in leaves.into_iter().enumerate() {
            clip_hashes.insert(filename, (index, leaf));
        }

        Self {
            clip_hashes,
            video_name: video_name.to_string(),
        }
    }

    pub fn generate_proof(&self, filename: &str) -> Option<MerkleVerification> {
        let Some(index) = self.clip_hashes.get(filename).map(|x| x.0) else {
            return None;
        };

        let mut leaves = vec![[0u8; 32]; self.clip_hashes.len()];
        for (index, leaf) in self.clip_hashes.values() {
            leaves[*index] = *leaf;
        }

        let merkle_tree = MerkleTree::<Sha256>::from_leaves(&leaves);

        let merkle_proof = merkle_tree.proof(&[index]).to_bytes();

        Some(MerkleVerification {
            merkle_proof,
            index,
            count: leaves.len(),
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct MerkleVerification {
    pub merkle_proof: Vec<u8>,
    pub index: usize,
    pub count: usize,
}
