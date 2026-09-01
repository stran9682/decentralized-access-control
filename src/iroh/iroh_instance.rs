use std::path::PathBuf;

use anyhow::Context;
use iroh::{Endpoint, SecretKey, endpoint::presets};
use iroh_blobs::{api::Store, store::fs::FsStore};
use iroh_docs::protocol::Docs;
use iroh_gossip::Gossip;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone)]
pub struct IrohInstance {
    store: FsStore,
    docs: Docs,
    endpoint: Endpoint,
}

impl IrohInstance {
    pub async fn new(path: PathBuf) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&path).await?;

        let key = load_secret_key(path.join("keypair")).await?;

        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(key)
            .bind()
            .await?;

        let blobs = FsStore::load(&path).await?;
        let gossip = Gossip::builder().spawn(endpoint.clone());

        let docs = Docs::persistent(path)
            .spawn(endpoint.clone(), (*blobs).clone(), gossip)
            .await?;

        Ok(Self {
            store: blobs,
            docs,
            endpoint,
        })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn blobs(&self) -> &Store {
        &self.store
    }

    pub fn docs(&self) -> &Docs {
        &self.docs
    }
}

pub async fn load_secret_key(key_path: PathBuf) -> anyhow::Result<SecretKey> {
    if key_path.exists() {
        let key_bytes = tokio::fs::read(key_path).await?;

        let secret_key = SecretKey::try_from(&key_bytes[0..32])?;
        Ok(secret_key)
    } else {
        let secret_key = SecretKey::generate();

        // Try to canonicalize if possible
        let key_path = key_path.canonicalize().unwrap_or(key_path);
        let key_path_parent = key_path.parent().ok_or_else(|| {
            anyhow::anyhow!("no parent directory found for '{}'", key_path.display())
        })?;
        tokio::fs::create_dir_all(&key_path_parent).await?;

        // write to tempfile
        let (file, temp_file_path) = tempfile::NamedTempFile::new_in(key_path_parent)
            .context("unable to create tempfile")?
            .into_parts();
        let mut file = tokio::fs::File::from_std(file);
        file.write_all(&secret_key.to_bytes())
            .await
            .context("unable to write keyfile")?;
        file.flush().await?;
        drop(file);

        // move file
        tokio::fs::rename(temp_file_path, key_path)
            .await
            .context("failed to rename keyfile")?;

        Ok(secret_key)
    }
}
