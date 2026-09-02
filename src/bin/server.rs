use std::{path::PathBuf, str::FromStr, sync::Arc};

use anyhow::Context;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::header,
    routing::get,
};
use decentralized_access_control::{
    ALPN, access_list::list_manager::AccessListManager, iroh::iroh_instance::IrohInstance,
    protocol::access_control::AccessControl, store::storage_manager::StorageManager,
};
use iroh::{EndpointId, protocol::Router as ARouter};
use iroh_docs::ALPN as DOCS_ALPN;
use serde::Deserialize;
use tokio_util::io::ReaderStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let iroh_instance = IrohInstance::new(PathBuf::new()).await?;

    let list_manager = AccessListManager::new(iroh_instance.clone());
    let storage_manager = StorageManager::new(iroh_instance.clone());

    let access_control = AccessControl::new(list_manager.clone(), storage_manager);

    let _router = ARouter::builder(iroh_instance.endpoint().clone())
        .accept(DOCS_ALPN, iroh_instance.docs().clone())
        .accept(ALPN, access_control.clone())
        .spawn();

    let access_control_service = AccessControlService::new(access_control);

    let app = Router::new()
        .route("/", get(download_handler))
        .with_state(Arc::new(access_control_service));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    return axum::serve(listener, app).await.context("Server failed");
}

#[derive(Deserialize)]
struct RequestArgs {
    resource: String,
    filename: String,
    endpoint_id: Option<String>,
}

struct AccessControlService {
    access_control: AccessControl,
}

impl AccessControlService {
    fn new(access_control: AccessControl) -> Self {
        Self { access_control }
    }

    pub async fn download_file(
        &self,
        resource: &str,
        filename: &str,
        endpoint_id: Option<EndpointId>,
    ) -> anyhow::Result<Body> {
        let file = self
            .access_control
            .make_request(endpoint_id, resource, filename)
            .await?;

        let stream = ReaderStream::new(file);
        let body = Body::from_stream(stream);

        Ok(body)
    }
}

async fn download_handler(
    Query(request_args): Query<RequestArgs>,
    State(access_control_service): State<Arc<AccessControlService>>,
) {
    let endpoint_id = if let Some(endpoint_id) = request_args.endpoint_id {
        iroh::EndpointId::from_str(&endpoint_id).ok()
    } else {
        None
    };

    let body = access_control_service
        .download_file(&request_args.resource, &request_args.filename, endpoint_id)
        .await;

    let headers = [(header::CONTENT_TYPE, "text/plain; charset=utf-8")];

    todo!()
}
