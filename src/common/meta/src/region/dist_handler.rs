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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::region::region_request::Body;
use api::v1::region::{
    DeleteRequests, InsertRequests, QueryRequest, RegionRequest, RegionRequestHeader,
};
use async_trait::async_trait;
use client::region_client::RegionClient;
use client::region_handler::{AffectedRows, RegionRequestHandler};
use client::{error as client_error, Result as ClientResult};
use common_error::ext::BoxedError;
use common_grpc::channel_manager::ChannelConfig;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::debug;
use futures::future;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use super::client_manager::DatanodeClients;
use crate::error::{JoinTaskSnafu, RegionLeaderNotFoundSnafu, Result};
use crate::key::table_route::TableRouteManager;
use crate::kv_backend::KvBackendRef;
use crate::peer::Peer;
use crate::rpc::router::find_region_leader;

pub struct DistRegionRequestHandler {
    table_route_manager: TableRouteManager,
    datanode_clients: Arc<DatanodeClients>,
}

#[async_trait]
impl RegionRequestHandler for DistRegionRequestHandler {
    async fn handle(&self, request: RegionRequest) -> ClientResult<AffectedRows> {
        let header = request
            .header
            .context(client_error::MissingFieldSnafu { field: "header" })?;
        let body = request
            .body
            .context(client_error::MissingFieldSnafu { field: "body" })?;
        match &body {
            Body::Inserts(_) => self.handle_inserts(header, body).await,
            Body::Deletes(_) => self.handle_deletes(header, body).await,

            Body::Create(request) => self.handle_inner(request.region_id, header, body).await,
            Body::Drop(request) => self.handle_inner(request.region_id, header, body).await,
            Body::Open(request) => self.handle_inner(request.region_id, header, body).await,
            Body::Close(request) => self.handle_inner(request.region_id, header, body).await,
            Body::Alter(request) => self.handle_inner(request.region_id, header, body).await,
            Body::Flush(request) => self.handle_inner(request.region_id, header, body).await,
            Body::Compact(request) => self.handle_inner(request.region_id, header, body).await,
        }
    }

    async fn do_get(&self, request: QueryRequest) -> ClientResult<SendableRecordBatchStream> {
        let peer = self.find_region_leader(request.region_id).await?;
        let client = self.datanode_clients.get_client(&peer).await;
        debug!("Sending QueryRequest to peer {peer:?}");
        RegionClient::new(client).handle_query(request).await
    }
}

macro_rules! group_requests_by_peer {
    ($handler: expr, $requests: expr) => {{
        let mut requests_by_peer = HashMap::new();
        for req in $requests {
            let peer = $handler.find_region_leader(req.region_id).await?;
            let peer_requests = requests_by_peer.entry(peer).or_insert_with(Vec::new);
            peer_requests.push(req);
        }
        requests_by_peer
    }};
}

macro_rules! handle_mutations {
    ($method: tt, $item_type: ty, $items_type: tt, $body_arm: tt) => {
        async fn $method(
            &self,
            header: RegionRequestHeader,
            body: Body,
        ) -> ClientResult<AffectedRows> {
            let requests = match body {
                Body::$body_arm($items_type { requests }) => requests,
                _ => unreachable!(),
            };
            let requests_by_peer = group_requests_by_peer!(self, requests);

            let mut tasks = Vec::with_capacity(requests_by_peer.len());
            for (peer, requests) in requests_by_peer {
                let request = RegionRequest {
                    header: Some(header.clone()),
                    body: Some(Body::$body_arm($items_type { requests })),
                };
                let client = self.datanode_clients.get_client(&peer).await;
                let task = common_runtime::spawn_write(async move {
                    debug!("Sending {} to peer {peer:?}", stringify!($body_arm));
                    RegionClient::new(client).handle(request).await
                });
                tasks.push(task);
            }

            let results = future::try_join_all(tasks)
                .await
                .context(JoinTaskSnafu)
                .map_err(BoxedError::new)
                .context(client_error::HandleRequestSnafu)?;
            let affected_rows = results.into_iter().sum::<ClientResult<AffectedRows>>()?;
            Ok(affected_rows)
        }
    };
}

impl DistRegionRequestHandler {
    pub fn new(channel_config: ChannelConfig, kv_backend: KvBackendRef) -> Self {
        Self {
            table_route_manager: TableRouteManager::new(kv_backend),
            datanode_clients: Arc::new(DatanodeClients::new(channel_config)),
        }
    }

    handle_mutations!(handle_inserts, InsertRequest, InsertRequests, Inserts);
    handle_mutations!(handle_deletes, DeleteRequest, DeleteRequests, Deletes);

    async fn handle_inner(
        &self,
        region_id: u64,
        header: RegionRequestHeader,
        body: Body,
    ) -> ClientResult<AffectedRows> {
        let peer = self.find_region_leader(region_id).await?;
        let client = self.datanode_clients.get_client(&peer).await;

        debug!(
            "Sending {} to region {region_id} on peer {peer:?}",
            body.as_ref()
        );

        let request = RegionRequest {
            header: Some(header),
            body: Some(body),
        };
        RegionClient::new(client).handle(request).await
    }

    async fn find_region_leader(&self, region_id: u64) -> ClientResult<Peer> {
        self.find_region_leader_inner(region_id)
            .await
            .map_err(BoxedError::new)
            .context(client_error::HandleRequestSnafu)
    }

    async fn find_region_leader_inner(&self, region_id: u64) -> Result<Peer> {
        let region_id = RegionId::from(region_id);
        let peer = self
            .table_route_manager
            .get(region_id.table_id())
            .await?
            .and_then(|route| {
                find_region_leader(&route.region_routes, region_id.region_number()).cloned()
            })
            .context(RegionLeaderNotFoundSnafu { region_id })?;
        Ok(peer)
    }
}
