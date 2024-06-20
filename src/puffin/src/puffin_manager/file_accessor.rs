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

use async_trait::async_trait;
use futures::{AsyncRead, AsyncSeek, AsyncWrite};

use crate::error::Result;

#[async_trait]
pub trait PuffinFileAccessor {
    type Reader: AsyncRead + AsyncSeek;
    type Writer: AsyncWrite;

    async fn reader(&self, puffin_file_name: &str) -> Result<Self::Reader>;

    /// TODO(must not exist)
    async fn writer(&self, puffin_file_name: &str) -> Result<Self::Writer>;
}

pub type PuffinFileAccessorRef<R, W> =
    Arc<dyn PuffinFileAccessor<Reader = R, Writer = W> + Send + Sync>;
