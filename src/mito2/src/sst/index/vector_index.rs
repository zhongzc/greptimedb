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

use std::collections::BTreeSet;
use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{VectorIndexMetric, VectorIndexOptions, VectorIndexType};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, VectorSearch};

use crate::error::Result;
use crate::read::Batch;
use crate::sst::file::FileId;

pub struct VectorIndexer {
    options: VectorIndexOptions,
    column_id: ColumnId,
    dim: usize,
    vectors: Vec<f32>,
    row_count: usize,
}

impl VectorIndexer {
    pub fn new(metadata: &RegionMetadataRef) -> Option<Self> {
        for column in &metadata.column_metadatas {
            if let Ok(Some(options)) = column.column_schema.vector_index_options() {
                if let ConcreteDataType::Vector(v) = column.column_schema.data_type {
                    let column_id = column.column_id;
                    let dim = v.dim;
                    let vectors = Vec::new();
                    return Some(Self {
                        options,
                        column_id,
                        dim,
                        vectors,
                        row_count: 0,
                    });
                }
            }
        }

        None
    }

    pub fn update(&mut self, batch: &Batch) {
        for field in batch.fields() {
            if field.column_id == self.column_id {
                for i in 0..batch.num_rows() {
                    let data = field.data.get(i);
                    let s = data.as_string().unwrap();
                    let vec = s
                        .trim_matches(|c| c == '[' || c == ']')
                        .split(',')
                        .map(|x| x.parse::<f32>().unwrap())
                        .collect::<Vec<f32>>();
                    assert_eq!(
                        vec.len(),
                        self.dim,
                        "Vector dimension mismatch, expected {}, got {}",
                        self.dim,
                        vec.len()
                    );
                    self.vectors.extend(vec);
                }
                self.row_count += batch.num_rows();
            }
        }
    }

    pub fn finish(&self, path: &str) {
        let params = options_to_params(&self.options, self.dim);
        let index =
            vsag_sys::VsagIndex::new(&self.options.index_type.to_string(), &params).unwrap();

        let ids = (0..self.row_count as i64).collect::<Vec<i64>>();
        let failed_ids = index
            .build(self.row_count, self.dim, &ids, &self.vectors)
            .unwrap();
        assert_eq!(failed_ids.len(), 0, "Existed failed ids: {:?}", failed_ids);

        index.dump(path).unwrap();
    }
}

pub struct VectorIndexApplier {
    region_dir: String,
    vector_search: VectorSearch,
    vecotr_index_options: VectorIndexOptions,
    dim: usize,
}

pub type VectorIndexApplierRef = Arc<VectorIndexApplier>;

impl VectorIndexApplier {
    pub fn arc(
        region_dir: String,
        vector_search: VectorSearch,
        region_metadata: &RegionMetadataRef,
    ) -> Option<VectorIndexApplierRef> {
        for column in &region_metadata.column_metadatas {
            if let Ok(Some(vecotr_index_options)) = column.column_schema.vector_index_options() {
                if let ConcreteDataType::Vector(d) = column.column_schema.data_type {
                    if vector_search
                        .column
                        .eq_ignore_ascii_case(&column.column_schema.name)
                    {
                        return Some(Arc::new(Self {
                            region_dir,
                            vector_search,
                            vecotr_index_options,
                            dim: d.dim,
                        }));
                    }
                }
            }
        }

        None
    }

    pub fn apply(&self, file_id: FileId) -> BTreeSet<u32> {
        let path = file_id.as_vector_index(&self.region_dir);
        let params = options_to_params(&self.vecotr_index_options, self.dim);
        let index_type = self.vecotr_index_options.index_type.to_string();
        let index = vsag_sys::VsagIndex::load(&path, &index_type, &params).unwrap();

        let param = match self.vecotr_index_options.index_type {
            VectorIndexType::Hnsw => "{\"hnsw\": {\"ef_search\": 100}}",
            VectorIndexType::Diskann => {
                "{\"diskann\": {\"ef_search\": 100, \"beam_search\": 4, \"io_limit\": 200}}"
            }
        };

        let output = index
            .knn_search(
                self.vector_search.vector.as_slice(),
                self.vector_search.top_k,
                param,
            )
            .unwrap();

        output.ids.into_iter().map(|a| a as u32).collect()
    }
}

fn options_to_params(options: &VectorIndexOptions, dim: usize) -> String {
    let params = match options.index_type {
        VectorIndexType::Hnsw => {
            "{
            \"max_degree\": 16,
            \"ef_construction\": 100
        }"
        }
        VectorIndexType::Diskann => {
            "{
            \"max_degree\": 16,
            \"ef_construction\": 100,
            \"pq_dims\": 32,
            \"pq_sample_rate\": 0.5
        }"
        }
    };
    let metric = match options.metric {
        VectorIndexMetric::L2sq => "l2",
        VectorIndexMetric::Cosine => "cosine",
    };

    format!(
        r#"{{
            "dtype": "float32",
            "metric_type": "{}",
            "dim": {},
            "{}": {}
        }}"#,
        metric, dim, options.index_type, params
    )
}
