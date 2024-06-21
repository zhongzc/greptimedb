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

mod cache_manager;
mod cached_puffin_manager;
mod file_accessor;

pub use cached_puffin_manager::{CachedPuffinManager, CachedPuffinReader, CachedPuffinWriter};

#[cfg(test)]
mod tests;

use std::path::PathBuf;

use async_trait::async_trait;
use futures::{AsyncRead, AsyncSeek};

use crate::blob_metadata::CompressionCodec;
use crate::error::Result;

#[async_trait]
pub trait PuffinManager {
    type Reader: PuffinReader;
    type Writer: PuffinWriter;

    async fn reader(&self, puffin_file_name: &str) -> Result<Self::Reader>;

    async fn writer(&self, puffin_file_name: &str) -> Result<Self::Writer>;
}

#[async_trait]
pub trait PuffinReader {
    type Reader: AsyncRead + AsyncSeek;

    async fn blob(&self, key: &str) -> Result<Self::Reader>;

    async fn dir(&self, key: &str) -> Result<PathBuf>;
}

#[async_trait]
pub trait PuffinWriter {
    async fn put_blob(
        &mut self,
        key: &str,
        raw_data: impl AsyncRead + Send,
        options: Option<PutOptions>,
    ) -> Result<()>;

    async fn put_dir(&mut self, key: &str, dir: PathBuf, options: Option<PutOptions>)
        -> Result<()>;

    fn set_footer_lz4_compressed(&mut self, lz4_compressed: bool);

    async fn finish(self) -> Result<()>;
}

pub struct PutOptions {
    pub data_compression: Option<CompressionCodec>,
}
