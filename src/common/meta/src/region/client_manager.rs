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
use std::time::Duration;

use client::Client;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use moka::future::{Cache, CacheBuilder};

use crate::peer::Peer;

pub struct DatanodeClients {
    channel_manager: ChannelManager,
    clients: Cache<Peer, Client>,
}

pub type DatanodeClientsRef = Arc<DatanodeClients>;

impl Default for DatanodeClients {
    fn default() -> Self {
        Self::new(ChannelConfig::default())
    }
}

impl DatanodeClients {
    pub fn new(config: ChannelConfig) -> Self {
        Self {
            channel_manager: ChannelManager::with_config(config),
            clients: CacheBuilder::new(1024)
                .time_to_live(Duration::from_secs(30 * 60))
                .time_to_idle(Duration::from_secs(5 * 60))
                .build(),
        }
    }

    pub async fn get_client(&self, datanode: &Peer) -> Client {
        self.clients
            .get_with_by_ref(datanode, async move {
                Client::with_manager_and_urls(
                    self.channel_manager.clone(),
                    vec![datanode.addr.clone()],
                )
            })
            .await
    }

    #[cfg(feature = "testing")]
    pub async fn insert_client(&self, datanode: Peer, client: Client) {
        self.clients.insert(datanode, client).await
    }
}
