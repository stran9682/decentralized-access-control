use std::{path::PathBuf, str::FromStr, sync::Arc};

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
    iroh::iroh_instance::IrohInstance,
    protocol::access_control::{AccessControl, Request},
    store::storage_manager::StorageManager,
};
use iroh::{EndpointId, protocol::Router as ARouter};
use iroh_docs::ALPN as DOCS_ALPN;
use serde::Deserialize;
use tokio::fs::File;
use tokio_util::io::ReaderStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let iroh_instance = IrohInstance::new(PathBuf::new()).await?;

    let list_manager = AccessListManager::new(iroh_instance.clone());
    let storage_manager = StorageManager::new(iroh_instance.clone());

    let access_control = AccessControl::new(
        list_manager.clone(),
        storage_manager,
        iroh_instance.endpoint().id(),
    );

    let _router = ARouter::builder(iroh_instance.endpoint().clone())
        .accept(DOCS_ALPN, iroh_instance.docs().clone())
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
    ) -> anyhow::Result<File> {
        let request = Request::new(1, String::from(resource), String::from(filename));

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
        .download_file(&request_args.resource, &request_args.filename, endpoint_id)
        .await
    {
        Ok(file) => file,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("File not found!"))
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
