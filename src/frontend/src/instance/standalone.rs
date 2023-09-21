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

use std::sync::Arc;

use api::v1::meta::Partition;
use api::v1::region::{QueryRequest, RegionRequest};
use async_trait::async_trait;
use client::error::{self as client_error, Result as ClientResult};
use client::region_client::check_response_header;
use client::region_handler::{AffectedRows, RegionRequestHandler};
use common_error::ext::BoxedError;
use common_meta::ddl::{TableMetadataAllocator, TableMetadataAllocatorContext};
use common_meta::error::Result as MetaResult;
use common_meta::kv_backend::KvBackendRef;
use common_meta::peer::Peer;
use common_meta::rpc::router::{Region, RegionRoute};
use common_meta::sequence::{Sequence, SequenceRef};
use common_recordbatch::SendableRecordBatchStream;
use datanode::region_server::RegionServer;
use servers::grpc::region_server::RegionServerHandler;
use snafu::{OptionExt, ResultExt};
use store_api::storage::{RegionId, TableId};
use table::metadata::RawTableInfo;

const TABLE_ID_SEQ: &str = "table_id";

pub struct StandaloneRegionRequestHandler {
    region_server: RegionServer,
}

#[async_trait]
impl RegionRequestHandler for StandaloneRegionRequestHandler {
    async fn handle(&self, request: RegionRequest) -> ClientResult<AffectedRows> {
        let body = request
            .body
            .context(client_error::MissingFieldSnafu { field: "body" })?;

        let response = self
            .region_server
            .handle(body)
            .await
            .map_err(BoxedError::new)
            .context(client_error::HandleRequestSnafu)?;

        check_response_header(response.header)?;
        Ok(response.affected_rows)
    }

    async fn do_get(&self, request: QueryRequest) -> ClientResult<SendableRecordBatchStream> {
        self.region_server
            .handle_read(request)
            .await
            .map_err(BoxedError::new)
            .context(client_error::HandleRequestSnafu)
    }
}

impl StandaloneRegionRequestHandler {
    pub fn arc(region_server: RegionServer) -> Arc<Self> {
        Arc::new(Self { region_server })
    }
}

pub(crate) struct StandaloneTableMetadataCreator {
    table_id_sequence: SequenceRef,
}

impl StandaloneTableMetadataCreator {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            table_id_sequence: Arc::new(Sequence::new(TABLE_ID_SEQ, 1024, 10, kv_backend)),
        }
    }
}

#[async_trait]
impl TableMetadataAllocator for StandaloneTableMetadataCreator {
    async fn create(
        &self,
        _ctx: &TableMetadataAllocatorContext,
        raw_table_info: &mut RawTableInfo,
        partitions: &[Partition],
    ) -> MetaResult<(TableId, Vec<RegionRoute>)> {
        let table_id = self.table_id_sequence.next().await? as u32;
        raw_table_info.ident.table_id = table_id;
        let region_routes = partitions
            .iter()
            .enumerate()
            .map(|(i, partition)| {
                let region = Region {
                    id: RegionId::new(table_id, i as u32),
                    partition: Some(partition.clone().into()),
                    ..Default::default()
                };
                // It's only a placeholder.
                let peer = Peer::default();
                RegionRoute {
                    region,
                    leader_peer: Some(peer),
                    follower_peers: vec![],
                }
            })
            .collect::<Vec<_>>();

        Ok((table_id, region_routes))
    }
}
