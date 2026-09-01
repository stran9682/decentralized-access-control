use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use axum::{Router, extract::{Path, State}, routing::get};
use decentralized_access_control::{
    ALPN,
    access_list::list_manager::AccessListManager,
    iroh::iroh_instance::IrohInstance,
    protocol::access_control::{self, AccessControl},
    store::storage_manager::StorageManager,
};
use iroh::{EndpointId, protocol::Router as ARouter};
use iroh_docs::ALPN as DOCS_ALPN;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let iroh_instance = IrohInstance::new(PathBuf::new()).await?;

    let list_manager = AccessListManager::new(iroh_instance.clone());
    let storage_manager = StorageManager::new(iroh_instance.clone());

    let access_control = AccessControl::new(list_manager.clone(), storage_manager);

    let router = ARouter::builder(iroh_instance.endpoint().clone())
        .accept(DOCS_ALPN, iroh_instance.docs().clone())
        .accept(ALPN, access_control.clone())
        .spawn();

    let access_control_service = AccessControlService::new(access_control);

    let app = Router::new()
        .route("/{tag}/{filename}", get(download_handler))
        .with_state(Arc::new(access_control_service));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    return axum::serve(listener, app).await.context("Server failed");
}

struct AccessControlService {
    access_control: AccessControl,
}

impl AccessControlService {
    fn new(access_control: AccessControl) -> Self {
        Self { access_control }
    }

    pub async fn download_file(&self, tag: &str, file_name: &str, endpoint_id: Option<EndpointId>) {
        
    }
}

async fn download_handler(
    Path((user_id, team_id)): Path<(String, String)>,
    State(state): State<Arc<AccessControlService>>
) {
    todo!()
}