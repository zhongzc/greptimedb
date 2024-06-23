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

use std::path::Path;

use async_trait::async_trait;
use snafu::ResultExt;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::store::{Compressor, ZstdCompressor};
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer, TokenizerManager};
use tantivy::{doc, Index, SingleSegmentIndexWriter};
use tantivy_jieba::JiebaTokenizer;

use crate::fulltext_index::create::FulltextIndexCreator;
use crate::fulltext_index::error::{Result, TantivySnafu};
use crate::fulltext_index::{Analyzer, Config};

const TEXT_FIELD_NAME: &str = "greptime_fulltext_text";
const ROW_ID_FIELD_NAME: &str = "greptime_fulltext_rowid";

// Port from tantivy::indexer::index_writer::{MEMORY_BUDGET_NUM_BYTES_MIN, MEMORY_BUDGET_NUM_BYTES_MAX}
const MARGIN_IN_BYTES: usize = 1_000_000;
const MEMORY_BUDGET_NUM_BYTES_MIN: usize = ((MARGIN_IN_BYTES as u32) * 15u32) as usize;
const MEMORY_BUDGET_NUM_BYTES_MAX: usize = u32::MAX as usize - MARGIN_IN_BYTES;

pub struct TantivyFulltextIndexCreator {
    writer: Option<SingleSegmentIndexWriter>,
    text_field: tantivy::schema::Field,
    count_field: tantivy::schema::Field,
    total_row_count: u64,
}

impl TantivyFulltextIndexCreator {
    // path should be an existing empty directory
    pub fn new(path: impl AsRef<Path>, config: Config, memory_limit: usize) -> Result<Self> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field(TEXT_FIELD_NAME, TEXT);
        let count_field = schema_builder.add_u64_field(ROW_ID_FIELD_NAME, STORED);
        let schema = schema_builder.build();

        let mut index = Index::create_in_dir(path, schema).context(TantivySnafu)?;
        index.settings_mut().docstore_compression = Compressor::Zstd(ZstdCompressor::default());
        index.set_tokenizers(Self::build_tokenizer(&config));

        let memory_limit = if memory_limit < MEMORY_BUDGET_NUM_BYTES_MIN {
            MEMORY_BUDGET_NUM_BYTES_MIN
        } else if memory_limit > MEMORY_BUDGET_NUM_BYTES_MAX {
            MEMORY_BUDGET_NUM_BYTES_MAX
        } else {
            memory_limit
        };

        let writer = SingleSegmentIndexWriter::new(index, memory_limit).context(TantivySnafu)?;
        Ok(Self {
            writer: Some(writer),
            text_field,
            count_field,
            total_row_count: 0,
        })
    }

    pub fn build_tokenizer(config: &Config) -> TokenizerManager {
        let mut builder = match config.analyzer {
            Analyzer::English => TextAnalyzer::builder(SimpleTokenizer::default()).dynamic(),
            Analyzer::Chinese => TextAnalyzer::builder(JiebaTokenizer {}).dynamic(),
        };

        if !config.case_sensitive {
            builder = builder.filter_dynamic(LowerCaser);
        }

        let tokenizer = builder.build();
        let tokenizer_manager = TokenizerManager::new();
        tokenizer_manager.register("default", tokenizer);
        tokenizer_manager
    }
}

#[async_trait]
impl FulltextIndexCreator for TantivyFulltextIndexCreator {
    async fn push_text(&mut self, text: &str) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer
                .add_document(doc!(
                    self.text_field => text,
                    self.count_field => self.total_row_count
                ))
                .context(TantivySnafu)?;
            self.total_row_count += 1;
        }

        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.finalize().context(TantivySnafu)?;
        }
        Ok(())
    }

    fn memory_usage(&self) -> usize {
        self.writer
            .as_ref()
            .map(|writer| writer.mem_usage())
            .unwrap_or(0)
    }
}
