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

mod moka_cache_manager;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::{AsyncRead, AsyncSeek, AsyncWrite};
pub use moka_cache_manager::MokaCacheManager;

use crate::error::Result;

pub type BoxedWriter = Box<dyn AsyncWrite + Unpin + Send>;
pub type FileInitFactory<'a> =
    Box<dyn FnOnce(BoxedWriter) -> BoxFuture<'static, Result<u64>> + Send + 'a>;

pub type WriterProvider = Box<dyn FnMut(String) -> BoxFuture<'static, Result<BoxedWriter>> + Send>;
pub type DirInitFactory<'a> =
    Box<dyn FnOnce(WriterProvider) -> BoxFuture<'static, Result<u64>> + Send + 'a>;

#[async_trait]
pub trait CacheManager {
    type Reader: AsyncRead + AsyncSeek;

    async fn get_blob(
        &self,
        puffin_file_name: &str,
        key: &str,
        init_factory: FileInitFactory<'_>,
    ) -> Result<Self::Reader>;

    async fn get_dir(
        &self,
        puffin_file_name: &str,
        key: &str,
        init_factory: DirInitFactory<'_>,
    ) -> Result<PathBuf>;

    async fn put_dir(
        &self,
        puffin_file_name: &str,
        key: &str,
        dir_path: PathBuf,
        size: u64,
    ) -> Result<()>;
}

pub type CacheManagerRef<R> = Arc<dyn CacheManager<Reader = R> + Send + Sync>;
