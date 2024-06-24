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

use api::v1::SemanticType;
use common_telemetry::warn;
use datatypes::scalars::ScalarVector;
use datatypes::schema::FulltextAnalyzer;
use datatypes::vectors::StringVector;
use index::fulltext_index::create::{FulltextIndexCreator, TantivyFulltextIndexCreator};
use index::fulltext_index::{Analyzer, Config};
use puffin::blob_metadata::CompressionCodec;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, ConcreteDataType, RegionId};
use tokio::fs;

use crate::error::{
    FulltextCleanupPathSnafu, FulltextOptionsSnafu, FulltextSnafu, FulltextTypeNotMatchSnafu,
    OperateAbortedIndexSnafu, PuffinAddBlobSnafu, Result,
};
use crate::read::Batch;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::INDEX_BLOB_TYPE;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount, Statistics};

pub struct SstIndexCreator {
    creators: HashMap<ColumnId, SingleCreator>,
    aborted: bool,
    /// Statistics of index creation.
    stats: Statistics,
}

struct SingleCreator {
    inner: Box<dyn FulltextIndexCreator>,
    column_id: ColumnId,
    path: PathBuf,
    compression_codec: Option<CompressionCodec>,
}

impl SstIndexCreator {
    pub async fn new(
        region_id: &RegionId,
        sst_file_id: &FileId,
        intermediate_manager: IntermediateManager,
        metadata: &RegionMetadataRef,
        compression_codec: Option<CompressionCodec>,
        memory_usage_threshold: usize,
    ) -> Result<Self> {
        let mut creators = HashMap::new();

        for column in &metadata.column_metadatas {
            let options =
                column
                    .column_schema
                    .fulltext_options()
                    .context(FulltextOptionsSnafu {
                        column_name: &column.column_schema.name,
                    })?;

            let options = match options {
                Some(options) if options.enable => options,
                _ => continue,
            };

            ensure!(
                column.column_schema.data_type == ConcreteDataType::string_datatype(),
                FulltextTypeNotMatchSnafu {
                    column_name: &column.column_schema.name,
                    data_type: &column.column_schema.data_type,
                }
            );

            let column_id = column.column_id;
            let path = intermediate_manager
                .fulltext_tmpdir_builder()
                .absolute_path(region_id, sst_file_id, &column_id);

            let config = Config {
                analyzer: match options.analyzer {
                    FulltextAnalyzer::English => Analyzer::English,
                    FulltextAnalyzer::Chinese => Analyzer::Chinese,
                },
                case_sensitive: options.case_sensitive,
            };

            tokio::fs::create_dir_all(&path).await.unwrap();
            let creator = TantivyFulltextIndexCreator::new(&path, config, memory_usage_threshold)
                .context(FulltextSnafu)?;

            creators.insert(
                column_id,
                SingleCreator {
                    inner: Box::new(creator),
                    column_id,
                    path,
                    compression_codec,
                },
            );
        }

        Ok(Self {
            creators,
            aborted: false,
            stats: Statistics::default(),
        })
    }

    pub async fn update(&mut self, batch: &Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if let Err(update_err) = self.do_update(batch).await {
            if let Err(err) = self.do_abort().await {
                if cfg!(any(test, feature = "test")) {
                    panic!("Failed to abort index creator, err: {err}");
                } else {
                    warn!(err; "Failed to abort index creator");
                }
            }
            return Err(update_err);
        }

        Ok(())
    }

    pub async fn finish(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
    ) -> Result<(RowCount, ByteCount)> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        match self.do_finish(puffin_writer).await {
            Ok(()) => Ok((self.stats.row_count(), self.stats.byte_count())),
            Err(finish_err) => {
                if let Err(err) = self.do_abort().await {
                    if cfg!(any(test, feature = "test")) {
                        panic!("Failed to abort index creator, err: {err}");
                    } else {
                        warn!(err; "Failed to abort index creator");
                    }
                }
                Err(finish_err)
            }
        }
    }

    pub async fn abort(&mut self) -> Result<()> {
        if self.aborted {
            return Ok(());
        }

        self.do_abort().await
    }

    async fn do_update(&mut self, batch: &Batch) -> Result<()> {
        let mut guard = self.stats.record_update();
        guard.inc_row_count(batch.num_rows());

        for (column_id, creator) in self.creators.iter_mut() {
            let text_column = batch.fields().iter().find(|c| c.column_id == *column_id);
            match text_column {
                Some(column) if column.data.data_type() == ConcreteDataType::string_datatype() => {
                    let data = column
                        .data
                        .as_any()
                        .downcast_ref::<StringVector>()
                        .expect("should match type");
                    for text in data.iter_data() {
                        creator
                            .inner
                            .push_text(text.unwrap_or_default())
                            .await
                            .context(FulltextSnafu)?;
                    }
                }
                _ => {
                    for _ in 0..batch.num_rows() {
                        creator.inner.push_text("").await.context(FulltextSnafu)?;
                    }
                }
            }
        }

        Ok(())
    }

    async fn do_finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<()> {
        let mut guard = self.stats.record_finish();

        let mut written_bytes = 0;
        for (col_id, creator) in self.creators.iter_mut() {
            creator.inner.finish().await.context(FulltextSnafu)?;
            let key = format!("{INDEX_BLOB_TYPE}-{col_id}");
            written_bytes += puffin_writer
                .put_dir(
                    &key,
                    creator.path.clone(),
                    Some(PutOptions {
                        data_compression: creator.compression_codec.clone(),
                    }),
                )
                .await
                .context(PuffinAddBlobSnafu)?;
        }

        guard.inc_byte_count(written_bytes);
        Ok(())
    }

    async fn do_abort(&mut self) -> Result<()> {
        let _guard = self.stats.record_cleanup();

        self.aborted = true;

        let mut first_err = None;
        for (col_id, creator) in self.creators.iter_mut() {
            if let Err(err) = creator.inner.finish().await {
                warn!(err; "Failed to finish fulltext index creator, col_id: {:?}, dir_path: {:?}", col_id, creator.path);
                first_err.get_or_insert(Err(err).context(FulltextSnafu));
            }
            if let Err(err) = fs::remove_dir_all(&creator.path).await {
                warn!(err; "Failed to remove fulltext index directory, col_id: {:?}, dir_path: {:?}", col_id, creator.path);
                first_err.get_or_insert(Err(err).context(FulltextCleanupPathSnafu));
            }
        }
        self.creators.clear();

        first_err.unwrap_or(Ok(()))
    }

    pub fn memory_usage(&self) -> usize {
        self.creators.values().map(|c| c.inner.memory_usage()).sum()
    }

    pub fn column_ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.creators.keys().copied()
    }
}
