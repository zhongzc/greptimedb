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
use std::path::PathBuf;

use common_telemetry::warn;
use datatypes::scalars::ScalarVector;
use datatypes::vectors::StringVector;
use futures::{AsyncRead, AsyncSeek, AsyncWrite};
use index::fulltext_index::create::FulltextIndexCreator;
use puffin::puffin_manager::{CachedPuffinWriter, PuffinWriter};
use snafu::{ensure, ResultExt};
use store_api::storage::{ColumnId, ConcreteDataType};
use tokio::fs;

use super::INDEX_BLOB_TYPE;
use crate::error::{
    FulltextCleanupPathSnafu, FulltextFinishSnafu, OperateAbortedIndexSnafu, Result,
};
use crate::read::Batch;

pub struct SstIndexCreator {
    creators: HashMap<ColumnId, (PathBuf, Box<dyn FulltextIndexCreator>)>,

    compression: bool,

    aborted: bool,
}

impl SstIndexCreator {
    pub async fn update(&mut self, batch: &Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        Ok(())
    }

    async fn do_update(&mut self, batch: &Batch) -> Result<()> {
        for (column_id, (_, creator)) in self.creators.iter_mut() {
            let text_column = batch.fields().iter().find(|c| c.column_id == *column_id);
            match text_column {
                Some(column) if column.data.data_type() == ConcreteDataType::string_datatype() => {
                    let data = column
                        .data
                        .as_any()
                        .downcast_ref::<StringVector>()
                        .expect("should match type");
                    for text in data.iter_data() {
                        creator.push_text(text.unwrap_or_default()).await.unwrap();
                    }
                }
                _ => {
                    for _ in 0..batch.num_rows() {
                        creator.push_text("").await.unwrap();
                    }
                }
            }
        }

        Ok(())
    }

    async fn do_finish<CR, W>(
        &mut self,
        puffin_writer: &mut CachedPuffinWriter<CR, W>,
    ) -> Result<()>
    where
        CR: AsyncRead + AsyncSeek,
        W: AsyncWrite + Unpin + Send,
    {
        for (col_id, (path, creator)) in self.creators.iter_mut() {
            creator.finish().await.unwrap();
            let key = format!("{INDEX_BLOB_TYPE}-{col_id}");
            puffin_writer
                .put_dir(&key, path.clone(), None)
                .await
                .unwrap();
        }

        Ok(())
    }

    async fn do_abort(&mut self) -> Result<()> {
        let mut first_err = None;
        for (col_id, (path, creator)) in self.creators.iter_mut() {
            if let Err(err) = creator.finish().await {
                warn!(err; "Failed to finish fulltext index creator, col_id: {:?}, dir_path: {:?}", col_id, path);
                first_err.get_or_insert(Err(err).context(FulltextFinishSnafu));
            }
            if let Err(err) = fs::remove_dir_all(&path).await {
                warn!(err; "Failed to remove fulltext index directory, col_id: {:?}, dir_path: {:?}", col_id, path);
                first_err.get_or_insert(Err(err).context(FulltextCleanupPathSnafu));
            }
        }

        first_err.unwrap_or(Ok(()))
    }
}
