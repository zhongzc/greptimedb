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

pub mod fulltext_index;
pub(crate) mod intermediate;
pub mod inverted_index;
pub(crate) mod puffin_manager;
mod statistics;
mod store;

use std::num::NonZeroUsize;
use std::path::PathBuf;

use common_telemetry::{debug, warn};
use object_store::ObjectStore;
use puffin::blob_metadata::CompressionCodec;
use puffin::puffin_manager::{PuffinManager as _, PuffinReader as _, PuffinWriter as _};
use puffin_manager::{SstPuffinManager, SstPuffinWriter};
use snafu::ResultExt;
use store::InstrumentedStore;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::BuildOn;
use crate::config::{FulltextIndexConfig, IndexConfig, InvertedIndexConfig};
use crate::error::PuffinSnafu;
use crate::metrics::INDEX_CREATE_MEMORY_USAGE;
use crate::read::Batch;
use crate::region::options::IndexOptions;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::creator::SstIndexCreator as FulltextIndexCreator;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::inverted_index::creator::SstIndexCreator as InvertedIndexCreator;

/// The index creator that hides the error handling details.
#[derive(Default)]
pub struct Indexer {
    file_path: String,
    file_id: FileId,
    region_id: RegionId,
    last_memory_usage: usize,

    puffin_writer: Option<SstPuffinWriter>,
    inverted_index_creator: Option<InvertedIndexCreator>,
    fulltext_index_creator: Option<FulltextIndexCreator>,
}

impl Indexer {
    async fn do_update(&mut self, batch: &Batch) -> bool {
        if batch.is_empty() {
            return true;
        }

        if !self.do_update_inverted_index(batch).await {
            return false;
        }

        if !self.do_update_fulltext_index(batch).await {
            return false;
        }

        return true;
    }

    async fn do_update_inverted_index(&mut self, batch: &Batch) -> bool {
        if let Some(inverted_index_creator) = self.inverted_index_creator.as_mut() {
            if let Err(err) = inverted_index_creator.update(batch).await {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to update inverted index, region_id: {}, file_id: {}, err: {}",
                        self.region_id, self.file_id, err
                    );
                } else {
                    warn!(
                        err; "Failed to update inverted index, skip creating index, region_id: {}, file_id: {}",
                        self.region_id, self.file_id,
                    );
                }

                return false;
            }
        }

        true
    }

    async fn do_update_fulltext_index(&mut self, batch: &Batch) -> bool {
        if let Some(fulltext_index_creator) = self.fulltext_index_creator.as_mut() {
            if let Err(err) = fulltext_index_creator.update(batch).await {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to update fulltext index, region_id: {}, file_id: {}, err: {}",
                        self.region_id, self.file_id, err
                    );
                } else {
                    warn!(
                        err; "Failed to update fulltext index, skip creating index, region_id: {}, file_id: {}",
                        self.region_id, self.file_id,
                    );
                }

                return false;
            }
        }

        true
    }
}

impl Indexer {
    async fn do_finish(&mut self) -> IndexOutput {
        let mut output = IndexOutput::default();

        let Some(mut puffin_writer) = self.puffin_writer.take() else {
            return output;
        };

        self.do_finish_inverted_index(&mut puffin_writer, &mut output)
            .await;

        self.do_finish_fulltext_index(&mut puffin_writer, &mut output)
            .await;

        output.file_size = self.do_finish_puffin_writer().await;
        output
    }

    async fn do_finish_inverted_index(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
        index_output: &mut IndexOutput,
    ) {
        if let Some(mut inverted_index_creator) = self.inverted_index_creator.take() {
            match inverted_index_creator.finish(puffin_writer).await {
                Ok((row_count, byte_count)) => {
                    debug!(
                        "Create inverted index successfully, region_id: {}, file_id: {}, written_bytes: {}, written_rows: {}",
                        self.region_id, self.file_id, byte_count, row_count
                    );

                    index_output.inverted_index.available = true;
                    index_output.inverted_index.written_bytes = byte_count;
                    index_output.fulltext_index.columns =
                        inverted_index_creator.column_ids().collect();
                }
                Err(err) => {
                    if cfg!(any(test, feature = "test")) {
                        panic!(
                            "Failed to create index, region_id: {}, file_id: {}, err: {}",
                            self.region_id, self.file_id, err
                        );
                    } else {
                        warn!(
                            err; "Failed to create index, region_id: {}, file_id: {}",
                            self.region_id, self.file_id,
                        );
                    }
                }
            }
        }
    }

    async fn do_finish_fulltext_index(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
        index_output: &mut IndexOutput,
    ) {
        if let Some(mut fulltext_index_creator) = self.fulltext_index_creator.take() {
            match fulltext_index_creator.finish(puffin_writer).await {
                Ok((row_count, byte_count)) => {
                    debug!(
                        "Create fulltext index successfully, region_id: {}, file_id: {}, written_bytes: {}, written_rows: {}",
                        self.region_id, self.file_id, byte_count, row_count
                    );

                    index_output.fulltext_index.available = true;
                    index_output.fulltext_index.written_bytes = byte_count;
                    index_output.fulltext_index.columns =
                        fulltext_index_creator.column_ids().collect();
                }
                Err(err) => {
                    if cfg!(any(test, feature = "test")) {
                        panic!(
                            "Failed to create fulltext index, region_id: {}, file_id: {}, err: {}",
                            self.region_id, self.file_id, err
                        );
                    } else {
                        warn!(
                            err; "Failed to create fulltext index, region_id: {}, file_id: {}",
                            self.region_id, self.file_id,
                        );
                    }
                }
            }
        }
    }

    async fn do_finish_puffin_writer(&mut self) -> u64 {
        if let Some(puffin_writer) = self.puffin_writer.take() {
            match puffin_writer.finish().await {
                Ok(size) => return size,
                Err(err) => {
                    if cfg!(any(test, feature = "test")) {
                        panic!(
                            "Failed to finish puffin writer, region_id: {}, file_id: {}, err: {}",
                            self.region_id, self.file_id, err
                        );
                    } else {
                        warn!(
                            err; "Failed to finish puffin writer, region_id: {}, file_id: {}",
                            self.region_id, self.file_id,
                        );
                    }
                }
            }
        }

        0
    }
}

impl Indexer {
    async fn do_abort(&mut self) {
        self.do_abort_inverted_index().await;
        self.do_abort_fulltext_index().await;
        self.do_finish_puffin_writer().await;
    }

    async fn do_abort_inverted_index(&mut self) {
        if let Some(mut inverted_index_creator) = self.inverted_index_creator.take() {
            if let Err(err) = inverted_index_creator.abort().await {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to abort inverted index, region_id: {}, file_id: {}, err: {}",
                        self.region_id, self.file_id, err
                    );
                } else {
                    warn!(
                        err; "Failed to abort inverted index, region_id: {}, file_id: {}",
                        self.region_id, self.file_id,
                    );
                }
            }
        }
    }

    async fn do_abort_fulltext_index(&mut self) {
        if let Some(mut fulltext_index_creator) = self.fulltext_index_creator.take() {
            if let Err(err) = fulltext_index_creator.abort().await {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to abort fulltext index, region_id: {}, file_id: {}, err: {}",
                        self.region_id, self.file_id, err
                    );
                } else {
                    warn!(
                        err; "Failed to abort fulltext index, region_id: {}, file_id: {}",
                        self.region_id, self.file_id,
                    );
                }
            }
        }
    }
}

impl Indexer {
    /// Update the index with the given batch.
    pub async fn update(&mut self, batch: &Batch) {
        if !self.do_update(batch).await {
            self.abort().await;
            return;
        }

        let memory_usage = self.memory_usage();
        INDEX_CREATE_MEMORY_USAGE.add(memory_usage as i64 - self.last_memory_usage as i64);
        self.last_memory_usage = memory_usage;
    }

    /// Finish the index creation.
    pub async fn finish(&mut self) -> IndexOutput {
        INDEX_CREATE_MEMORY_USAGE.sub(self.last_memory_usage as i64);
        self.last_memory_usage = 0;

        self.do_finish().await
    }

    /// Abort the index creation.
    pub async fn abort(&mut self) {
        INDEX_CREATE_MEMORY_USAGE.sub(self.last_memory_usage as i64);
        self.last_memory_usage = 0;

        self.do_abort().await;
    }

    fn memory_usage(&self) -> usize {
        self.inverted_index_creator
            .as_ref()
            .map_or(0, |creator| creator.memory_usage())
            + self
                .fulltext_index_creator
                .as_ref()
                .map_or(0, |creator| creator.memory_usage())
    }
}

#[derive(Debug, Clone, Default)]
pub struct IndexOutput {
    pub file_size: u64,
    pub inverted_index: InvertedIndexOutput,
    pub fulltext_index: FulltextIndexOutput,
}

#[derive(Debug, Clone, Default)]
pub struct InvertedIndexOutput {
    pub available: bool,
    pub written_bytes: u64,
    pub columns: Vec<ColumnId>,
}

#[derive(Debug, Clone, Default)]
pub struct FulltextIndexOutput {
    pub available: bool,
    pub written_bytes: u64,
    pub columns: Vec<ColumnId>,
}

pub(crate) struct IndexerBuilder<'a> {
    pub(crate) build_on: BuildOn,

    pub(crate) file_id: FileId,
    pub(crate) metadata: &'a RegionMetadataRef,
    pub(crate) row_group_size: usize,

    pub(crate) puffin_manager: SstPuffinManager,
    pub(crate) intermediate_manager: IntermediateManager,
    pub(crate) file_path: String,

    pub(crate) index_options: IndexOptions,
    pub(crate) index_config: IndexConfig,
    pub(crate) inverted_index_config: InvertedIndexConfig,
    pub(crate) fulltext_index_config: FulltextIndexConfig,
}

impl<'a> IndexerBuilder<'a> {
    /// Sanity check for arguments and create a new [Indexer]
    /// with inner [SstIndexCreator] if arguments are valid.
    pub(crate) async fn build(self) -> Indexer {
        let mut indexer = Indexer {
            file_path: self.file_path.clone(),
            file_id: self.file_id,
            region_id: self.metadata.region_id,
            last_memory_usage: 0,

            ..Default::default()
        };

        indexer.inverted_index_creator = self.build_inverted_indexer();
        indexer.fulltext_index_creator = self.build_fulltext_indexer().await;

        if indexer.inverted_index_creator.is_none() && indexer.fulltext_index_creator.is_none() {
            indexer.abort().await;
            return Indexer::default();
        }

        match self.build_puffin_writer().await {
            Some(writer) => indexer.puffin_writer = Some(writer),
            None => return Indexer::default(),
        }

        indexer
    }

    async fn build_puffin_writer(&self) -> Option<SstPuffinWriter> {
        let puffin_writer = match self.puffin_manager.writer(&self.file_path).await {
            Ok(writer) => writer,
            Err(err) => {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to create puffin writer, region_id: {}, file_id: {}, err: {}",
                        self.metadata.region_id, self.file_id, err
                    );
                } else {
                    warn!(
                        err; "Failed to create puffin writer, region_id: {}, file_id: {}",
                        self.metadata.region_id, self.file_id,
                    );
                }

                return None;
            }
        };

        Some(puffin_writer)
    }

    fn build_inverted_indexer(&self) -> Option<InvertedIndexCreator> {
        let create = match self.build_on {
            BuildOn::Flush => self.inverted_index_config.create_on_flush.auto(),
            BuildOn::Compaction => self.inverted_index_config.create_on_compaction.auto(),
        };

        if !create {
            debug!(
                "Skip creating index due to request, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        }

        if self.metadata.primary_key.is_empty() {
            debug!(
                "No tag columns, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        }

        let Some(mut segment_row_count) =
            NonZeroUsize::new(self.index_options.inverted_index.segment_row_count)
        else {
            warn!(
                "Segment row count is 0, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        };

        let Some(row_group_size) = NonZeroUsize::new(self.row_group_size) else {
            warn!(
                "Row group size is 0, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        };

        // if segment row count not aligned with row group size, adjust it to be aligned.
        if row_group_size.get() % segment_row_count.get() != 0 {
            segment_row_count = row_group_size;
        }

        let mem_threshold = self
            .inverted_index_config
            .mem_threshold_on_create
            .map(|t| t.as_bytes() as usize);
        let compression_codec = self
            .inverted_index_config
            .compress
            .then_some(CompressionCodec::Zstd);

        let creator = InvertedIndexCreator::new(
            self.file_id,
            self.metadata,
            self.intermediate_manager.clone(),
            mem_threshold,
            segment_row_count,
            compression_codec,
        )
        .with_ignore_column_ids(
            self.index_options
                .inverted_index
                .ignore_column_ids
                .iter()
                .map(|i| i.to_string())
                .collect(),
        );

        Some(creator)
    }

    async fn build_fulltext_indexer(&self) -> Option<FulltextIndexCreator> {
        let create = match self.build_on {
            BuildOn::Flush => self.fulltext_index_config.create_on_flush.auto(),
            BuildOn::Compaction => self.fulltext_index_config.create_on_compaction.auto(),
        };

        if !create {
            debug!(
                "Skip creating fulltext index due to request, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        }

        let compression_codec = self
            .fulltext_index_config
            .compress
            .then_some(CompressionCodec::Zstd);
        let mem_threshold = self
            .fulltext_index_config
            .mem_threshold_on_create
            .as_bytes() as usize;
        let creator = FulltextIndexCreator::new(
            &self.metadata.region_id,
            &self.file_id,
            self.intermediate_manager.clone(),
            &self.metadata,
            compression_codec,
            mem_threshold,
        )
        .await;

        match creator {
            Ok(creator) => Some(creator),
            Err(err) => {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to create fulltext index creator, region_id: {}, file_id: {}, err: {}",
                        self.metadata.region_id, self.file_id, err
                    );
                } else {
                    warn!(
                        err; "Failed to create fulltext index creator, region_id: {}, file_id: {}",
                        self.metadata.region_id, self.file_id,
                    );
                }

                None
            }
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use api::v1::SemanticType;
//     use datatypes::data_type::ConcreteDataType;
//     use datatypes::schema::ColumnSchema;
//     use object_store::services::Memory;
//     use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

//     use super::*;

//     fn mock_region_metadata() -> RegionMetadataRef {
//         let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
//         builder
//             .push_column_metadata(ColumnMetadata {
//                 column_schema: ColumnSchema::new("a", ConcreteDataType::int64_datatype(), false),
//                 semantic_type: SemanticType::Tag,
//                 column_id: 1,
//             })
//             .push_column_metadata(ColumnMetadata {
//                 column_schema: ColumnSchema::new("b", ConcreteDataType::float64_datatype(), false),
//                 semantic_type: SemanticType::Field,
//                 column_id: 2,
//             })
//             .push_column_metadata(ColumnMetadata {
//                 column_schema: ColumnSchema::new(
//                     "c",
//                     ConcreteDataType::timestamp_millisecond_datatype(),
//                     false,
//                 ),
//                 semantic_type: SemanticType::Timestamp,
//                 column_id: 3,
//             })
//             .primary_key(vec![1]);

//         Arc::new(builder.build().unwrap())
//     }

//     fn no_tag_region_metadata() -> RegionMetadataRef {
//         let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
//         builder
//             .push_column_metadata(ColumnMetadata {
//                 column_schema: ColumnSchema::new("a", ConcreteDataType::int64_datatype(), false),
//                 semantic_type: SemanticType::Field,
//                 column_id: 1,
//             })
//             .push_column_metadata(ColumnMetadata {
//                 column_schema: ColumnSchema::new("b", ConcreteDataType::float64_datatype(), false),
//                 semantic_type: SemanticType::Field,
//                 column_id: 2,
//             })
//             .push_column_metadata(ColumnMetadata {
//                 column_schema: ColumnSchema::new(
//                     "c",
//                     ConcreteDataType::timestamp_millisecond_datatype(),
//                     false,
//                 ),
//                 semantic_type: SemanticType::Timestamp,
//                 column_id: 3,
//             });

//         Arc::new(builder.build().unwrap())
//     }

//     fn mock_object_store() -> ObjectStore {
//         ObjectStore::new(Memory::default()).unwrap().finish()
//     }

//     fn mock_intm_mgr() -> IntermediateManager {
//         IntermediateManager::new(mock_object_store())
//     }

//     #[test]
//     fn test_build_indexer_basic() {
//         let metadata = mock_region_metadata();
//         let indexer = IndexerBuilder {
//             create_inverted_index: true,
//             mem_threshold_index_create: Some(1024),
//             write_buffer_size: None,
//             file_id: FileId::random(),
//             file_path: "test".to_string(),
//             metadata: &metadata,
//             row_group_size: 1024,
//             object_store: mock_object_store(),
//             intermediate_manager: mock_intm_mgr(),
//             index_options: IndexOptions::default(),
//         }
//         .build();

//         assert!(indexer.inverted_index_creator.is_some());
//     }

//     #[test]
//     fn test_build_indexer_disable_create() {
//         let metadata = mock_region_metadata();
//         let indexer = IndexerBuilder {
//             create_inverted_index: false,
//             mem_threshold_index_create: Some(1024),
//             write_buffer_size: None,
//             file_id: FileId::random(),
//             file_path: "test".to_string(),
//             metadata: &metadata,
//             row_group_size: 1024,
//             object_store: mock_object_store(),
//             intermediate_manager: mock_intm_mgr(),
//             index_options: IndexOptions::default(),
//         }
//         .build();

//         assert!(indexer.inverted_index_creator.is_none());
//     }

//     #[test]
//     fn test_build_indexer_no_tag() {
//         let metadata = no_tag_region_metadata();
//         let indexer = IndexerBuilder {
//             create_inverted_index: true,
//             mem_threshold_index_create: Some(1024),
//             write_buffer_size: None,
//             file_id: FileId::random(),
//             file_path: "test".to_string(),
//             metadata: &metadata,
//             row_group_size: 1024,
//             object_store: mock_object_store(),
//             intermediate_manager: mock_intm_mgr(),
//             index_options: IndexOptions::default(),
//         }
//         .build();

//         assert!(indexer.inverted_index_creator.is_none());
//     }

//     #[test]
//     fn test_build_indexer_zero_row_group() {
//         let metadata = mock_region_metadata();
//         let indexer = IndexerBuilder {
//             create_inverted_index: true,
//             mem_threshold_index_create: Some(1024),
//             write_buffer_size: None,
//             file_id: FileId::random(),
//             file_path: "test".to_string(),
//             metadata: &metadata,
//             row_group_size: 0,
//             object_store: mock_object_store(),
//             intermediate_manager: mock_intm_mgr(),
//             index_options: IndexOptions::default(),
//         }
//         .build();

//         assert!(indexer.inverted_index_creator.is_none());
//     }
// }
