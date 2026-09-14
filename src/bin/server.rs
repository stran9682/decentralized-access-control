use std::{str::FromStr, sync::Arc};

use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use decentralized_access_control::{
    ALPN,
    access_list::list_manager::AccessListManager,
    // discovery::discovery_service::DiscoveryService,
    iroh::iroh_mem_instance::IrohMemInstance,
    protocol::access_control::{AccessControl, Request},
    store::storage_manager::StorageManager,
};
use iroh::{Endpoint, EndpointId, endpoint::presets, protocol::Router as ARouter};
use iroh_blobs::{ALPN as BLOBS_ALPN, BlobsProtocol, store::mem::MemStore};
use iroh_docs::{ALPN as DOCS_ALPN, protocol::Docs};
use iroh_gossip::{ALPN as GOSSIP_ALPN, Gossip};
use serde::Deserialize;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // let iroh_instance = IrohInstance::new(PathBuf::new()).await?;
    let endpoint = Endpoint::bind(presets::N0).await?;
    let acl_blobs = MemStore::new();
    let storage_blobs = MemStore::new();
    let gossip = Gossip::builder().spawn(endpoint.clone());

    let docs = Docs::memory()
        .spawn(endpoint.clone(), (*acl_blobs).clone(), gossip.clone())
        .await?;

    let acl_iroh = IrohMemInstance::new(acl_blobs.clone(), docs.clone(), endpoint.clone());
    let storage_iroh = IrohMemInstance::new(storage_blobs, docs.clone(), endpoint.clone());

    let list_manager = AccessListManager::new(acl_iroh.clone());
    let storage_manager = StorageManager::new(storage_iroh);
    //let discovery_service = DiscoveryService::new(endpoint.clone(), gossip.clone());

    let access_control = AccessControl::new(list_manager.clone(), storage_manager, endpoint.id());

    let _router = ARouter::builder(endpoint)
        .accept(DOCS_ALPN, docs)
        .accept(GOSSIP_ALPN, gossip)
        .accept(BLOBS_ALPN, BlobsProtocol::new(&acl_blobs, None))
        .accept(ALPN, access_control.clone())
        .spawn();

    let access_control_service = AccessControlService::new(access_control);

    let app = Router::new()
        .route("/", get(download_handler))
        .with_state(Arc::new(access_control_service));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    axum::serve(listener, app).await?;

    return Ok(());
}

#[derive(Deserialize)]
struct RequestArgs {
    namespace: String,
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
        namespace: &str,
        resource: &str,
        filename: &str,
        endpoint_id: Option<EndpointId>,
    ) -> anyhow::Result<Option<File>> {
        let request = Request::new(
            String::from(namespace),
            String::from(resource),
            String::from(filename),
        );

        let file = self
            .access_control
            .make_request(endpoint_id, &request)
            .await?;

        Ok(file)
    }
}

async fn download_handler(
    Query(request_args): Query<RequestArgs>,
    State(access_control_service): State<Arc<AccessControlService>>,
) -> impl IntoResponse {
    let endpoint_id = if let Some(endpoint_id) = request_args.endpoint_id {
        iroh::EndpointId::from_str(&endpoint_id).ok()
    } else {
        None
    };

    let file = match access_control_service
        .download_file(
            &request_args.namespace,
            &request_args.resource,
            &request_args.filename,
            endpoint_id,
        )
        .await
    {
        Ok(Some(file)) => file,
        Ok(None) => {
            return Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::from(
                    "Permission error or file hash didn't match resource",
                ))
                .unwrap();
        }
        Err(e) => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(format!("Network error occured, {e}")))
                .unwrap();
        }
    };

    let content_type = mime_guess::from_path(&request_args.filename).first_or_octet_stream();

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type.as_ref())
        .body(body)
        .unwrap()
}
