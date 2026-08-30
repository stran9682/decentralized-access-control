use std::path::PathBuf;

use gossip_simulation::iroh::iroh_instance::IrohInstance;
use iroh::protocol::Router;
use iroh_docs::ALPN as DOCS_ALPN;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let iroh_instance = IrohInstance::new(PathBuf::new()).await?;

    let router = Router::builder(iroh_instance.endpoint().clone())
        .accept(DOCS_ALPN, iroh_instance.docs().clone())
        .spawn();

    todo!()
}
