// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::{routing, Json, Router};
use common_error::ext::ErrorExt;
use futures::future;
use hyper::StatusCode;
use serde_json::json;
use servers::grpc::{GrpcServer, GrpcServerConfig};
use servers::http::{HttpServer, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::server::Server;
use snafu::ResultExt;
use store_api::region_request::{RegionBuildIndexRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::DatanodeOptions;
use crate::error::{
    ParseAddrSnafu, Result, ShutdownServerSnafu, StartServerSnafu, WaitForGrpcServingSnafu,
};
use crate::region_server::RegionServer;

/// All rpc services.
pub struct Services {
    grpc_server: GrpcServer,
    http_server: HttpServer,
}

impl Services {
    pub async fn try_new(region_server: RegionServer, opts: &DatanodeOptions) -> Result<Self> {
        let flight_handler = Some(Arc::new(region_server.clone()) as _);
        let region_server_handler = Some(Arc::new(region_server.clone()) as _);
        let runtime = region_server.runtime();
        let grpc_config = GrpcServerConfig {
            max_recv_message_size: opts.rpc_max_recv_message_size.as_bytes() as usize,
            max_send_message_size: opts.rpc_max_send_message_size.as_bytes() as usize,
        };

        Ok(Self {
            grpc_server: GrpcServer::new(
                Some(grpc_config),
                None,
                None,
                flight_handler,
                region_server_handler,
                None,
                runtime,
            ),
            http_server: HttpServerBuilder::new(opts.http.clone())
                .with_metrics_handler(MetricsHandler)
                .with_greptime_config_options(opts.to_toml_string())
                .push_register(Arc::new(move |router| {
                    router.nest(
                        "/build-index",
                        Router::new()
                            .route("/:region_id", routing::post(build_index_handler))
                            .with_state(region_server.clone()),
                    )
                }))
                .build(),
        })
    }

    pub async fn start(&mut self, opts: &DatanodeOptions) -> Result<()> {
        let grpc_addr: SocketAddr = opts.rpc_addr.parse().context(ParseAddrSnafu {
            addr: &opts.rpc_addr,
        })?;
        let http_addr = opts.http.addr.parse().context(ParseAddrSnafu {
            addr: &opts.http.addr,
        })?;
        let grpc = self.grpc_server.start(grpc_addr);
        let http = self.http_server.start(http_addr);
        let _ = future::try_join_all(vec![grpc, http])
            .await
            .context(StartServerSnafu)?;

        self.grpc_server
            .wait_for_serve()
            .await
            .context(WaitForGrpcServingSnafu)?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.grpc_server
            .shutdown()
            .await
            .context(ShutdownServerSnafu)?;
        self.http_server
            .shutdown()
            .await
            .context(ShutdownServerSnafu)?;
        Ok(())
    }
}

#[axum_macros::debug_handler]
pub async fn build_index_handler(
    State(region_server): State<RegionServer>,
    Path(region_id): Path<RegionId>,
) -> axum::response::Response {
    let res = region_server
        .handle_request(
            region_id,
            RegionRequest::BuildIndex(RegionBuildIndexRequest {}),
        )
        .await;

    match res {
        Ok(_) => StatusCode::OK.into_response(),
        Err(err) => {
            let body = Json(json!({
                "error": err.output_msg()
            }));
            (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
        }
    }
}
