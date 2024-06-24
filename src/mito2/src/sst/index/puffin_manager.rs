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

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use puffin::error::{self as puffin_error, Result as PuffinResult};
use puffin::puffin_manager::{
    CachedPuffinManager, CachedPuffinReader, CachedPuffinWriter, MokaCacheManager,
    PuffinFileAccessor,
};
use snafu::ResultExt;

use crate::error::{PuffinSnafu, Result};
use crate::metrics::{
    INDEX_PUFFIN_FLUSH_OP_TOTAL, INDEX_PUFFIN_READ_BYTES_TOTAL, INDEX_PUFFIN_READ_OP_TOTAL,
    INDEX_PUFFIN_SEEK_OP_TOTAL, INDEX_PUFFIN_WRITE_BYTES_TOTAL, INDEX_PUFFIN_WRITE_OP_TOTAL,
};
use crate::sst::index::store::{InstrumentedAsyncRead, InstrumentedAsyncWrite, InstrumentedStore};

pub type CacheReader = tokio_util::compat::Compat<tokio::fs::File>;
type AsyncReader = InstrumentedAsyncRead<'static, object_store::FuturesAsyncReader>;
type AsyncWriter = InstrumentedAsyncWrite<'static, object_store::FuturesAsyncWriter>;
pub type SstPuffinReader = CachedPuffinReader<CacheReader, AsyncReader, AsyncWriter>;
pub type SstPuffinWriter = CachedPuffinWriter<CacheReader, AsyncWriter>;
pub type SstPuffinManager = CachedPuffinManager<CacheReader, AsyncReader, AsyncWriter>;
// pub type SstPuffinManagerRef = Arc<SstPuffinManager>;

#[derive(Clone)]
pub struct PuffinManagerFactory {
    cache_manager: Arc<MokaCacheManager>,
    write_buffer_size: Option<usize>,
}

impl PuffinManagerFactory {
    pub async fn new(
        cache_root_path: PathBuf,
        cache_size: u64,
        write_buffer_size: Option<usize>,
    ) -> Result<Self> {
        let cache_manager = MokaCacheManager::new(cache_root_path, cache_size)
            .await
            .context(PuffinSnafu)?;
        Ok(Self {
            cache_manager: Arc::new(cache_manager),
            write_buffer_size,
        })
    }

    pub(crate) fn build(&self, store: object_store::ObjectStore) -> SstPuffinManager {
        let store = InstrumentedStore::new(store).with_write_buffer_size(self.write_buffer_size);
        let puffin_file_accessor = ObjectStorePuffinFileAccessor::new(store);
        CachedPuffinManager::new(self.cache_manager.clone(), Arc::new(puffin_file_accessor))
    }
}

pub(crate) struct ObjectStorePuffinFileAccessor {
    object_store: InstrumentedStore,
}

impl ObjectStorePuffinFileAccessor {
    pub fn new(object_store: InstrumentedStore) -> Self {
        Self { object_store }
    }
}

#[async_trait]
impl PuffinFileAccessor for ObjectStorePuffinFileAccessor {
    type Reader = AsyncReader;
    type Writer = AsyncWriter;

    async fn reader(&self, puffin_file_name: &str) -> PuffinResult<Self::Reader> {
        self.object_store
            .reader(
                puffin_file_name,
                &INDEX_PUFFIN_READ_BYTES_TOTAL,
                &INDEX_PUFFIN_READ_OP_TOTAL,
                &INDEX_PUFFIN_SEEK_OP_TOTAL,
            )
            .await
            .map_err(BoxedError::new)
            .context(puffin_error::ExternalSnafu)
    }

    async fn writer(&self, puffin_file_name: &str) -> PuffinResult<Self::Writer> {
        self.object_store
            .writer(
                puffin_file_name,
                &INDEX_PUFFIN_WRITE_BYTES_TOTAL,
                &INDEX_PUFFIN_WRITE_OP_TOTAL,
                &INDEX_PUFFIN_FLUSH_OP_TOTAL,
            )
            .await
            .map_err(BoxedError::new)
            .context(puffin_error::ExternalSnafu)
    }
}
