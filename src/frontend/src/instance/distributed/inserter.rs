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

use api::v1::region::{region_request, InsertRequests};
use client::region::RegionRequester;
use common_meta::peer::Peer;
use futures_util::future;
use metrics::counter;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    FindDatanodeSnafu, FindTableRouteSnafu, JoinTaskSnafu, RequestDatanodeSnafu, Result,
    SplitInsertSnafu,
};

pub struct DistInserter {
    ctx: QueryContextRef,
    catalog_manager: Arc<FrontendCatalogManager>,
}

impl DistInserter {
    pub fn new(ctx: QueryContextRef, catalog_manager: Arc<FrontendCatalogManager>) -> Self {
        Self {
            ctx,
            catalog_manager,
        }
    }

    pub(crate) async fn insert(&self, requests: InsertRequests) -> Result<u64> {
        let requests = self.split(requests).await?;
        let results = future::try_join_all(requests.into_iter().map(|(peer, inserts)| {
            let datanode_clients = self.catalog_manager.datanode_clients();
            let trace_id = self.ctx.trace_id();

            common_runtime::spawn_write(async move {
                let client = datanode_clients.get_client(&peer).await;

                let requester = RegionRequester::new(client, trace_id, 0);
                requester
                    .handle(region_request::Body::Inserts(inserts))
                    .await
                    .context(RequestDatanodeSnafu)
            })
        }))
        .await
        .context(JoinTaskSnafu)?;

        let affected_rows = results.into_iter().sum::<Result<u64>>()?;
        counter!(crate::metrics::DIST_INGEST_ROW_COUNT, affected_rows);
        Ok(affected_rows)
    }

    async fn split(&self, requests: InsertRequests) -> Result<HashMap<Peer, InsertRequests>> {
        let partition_manager = self.catalog_manager.partition_manager();
        let mut inserts: HashMap<Peer, InsertRequests> = HashMap::new();

        for req in requests.requests {
            let table_id = RegionId::from_u64(req.region_id).table_id();

            let req_splits = partition_manager
                .split_insert_request(table_id, req)
                .await
                .context(SplitInsertSnafu)?;
            let table_route = partition_manager
                .find_table_route(table_id)
                .await
                .context(FindTableRouteSnafu { table_id })?;

            for (region_number, insert) in req_splits {
                let peer =
                    table_route
                        .find_region_leader(region_number)
                        .context(FindDatanodeSnafu {
                            region: region_number,
                        })?;
                inserts
                    .entry(peer.clone())
                    .or_default()
                    .requests
                    .push(insert);
            }
        }

        Ok(inserts)
    }
}
