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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use common_query::Output;
use common_telemetry::{debug, error, info, warn};
use datatypes::value::Value;
use object_store::BlockingWriter;
use parquet::column::reader::get_typed_column_reader;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{SerializedFileReader, SerializedRowGroupReader};
use parquet::record::Field;
use parquet::schema::types::GroupTypeBuilder;
use snafu::ResultExt;
use store_api::metadata::{RegionMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::region_request::RegionAlterRequest;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use store_api::storage::RegionId;

use crate::error::{InvalidMetadataSnafu, InvalidRegionRequestSnafu, Result};
use crate::flush::FlushReason;
use crate::manifest::action::{RegionChange, RegionMetaAction, RegionMetaActionList};
use crate::memtable::MemtableBuilderRef;
use crate::region::version::Version;
use crate::region::MitoRegionRef;
use crate::request::{DdlRequest, OptionOutputTx, SenderDdlRequest};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};
use crate::sst::file::FileId;
use crate::sst::parquet::reader::ParquetReader;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_build_index_request(
        &mut self,
        region_id: RegionId,
        mut sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.writable_region_or(region_id, &mut sender) else {
            return;
        };

        let pk_codec = McmpRowCodec::new(
            region
                .version()
                .metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );
        sender.send(IndexWriter::new(region, pk_codec).process().await)
    }
}

pub struct IndexWriter {
    region: MitoRegionRef,
    pk_codec: McmpRowCodec,
}

impl IndexWriter {
    fn new(region: MitoRegionRef, pk_codec: McmpRowCodec) -> Self {
        Self { region, pk_codec }
    }

    async fn process(&self) -> Result<Output> {
        let current_version = self.region.version_control.current();
        for level in current_version.version.ssts.levels() {
            for (file_id, file_handle) in &level.files {
                let parquet_file_reader = self
                    .region
                    .access_layer
                    .sst_file_reader(file_handle.clone());
                let index_writer = self.region.access_layer.new_index_writer(file_id);
                let Some(index_writer) = index_writer else {
                    info!("Index for file {} already exists, skip", file_id);
                    continue;
                };
                self.build_index(file_id, parquet_file_reader, index_writer)
                    .await;
            }
        }

        Ok(Output::AffectedRows(0))
    }

    async fn build_index(
        &self,
        file_id: &FileId,
        parquet_file_reader: Arc<dyn FileReader>,
        mut index_writer: BlockingWriter,
    ) {
        info!("Building index for file {}", file_id);

        let column_index_builders = self
            .region
            .metadata()
            .primary_key_columns()
            .map(|column| ColumnIndexBuilder {
                column_name: column.column_schema.name.clone(),
                value_to_row_groups: BTreeMap::new(),
            })
            .collect::<Vec<_>>();

        let column_values = (0..parquet_file_reader.num_row_groups())
            .map(|row_group| {
                let region = self.region.clone();
                let parquet_file_reader = parquet_file_reader.clone();
                let pk_codec = self.pk_codec.clone();
                std::thread::spawn(move || {
                    column_values_to_row_group(region, parquet_file_reader, row_group, pk_codec)
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|t| t.join().expect("Failed to join thread"));

        let column_index_builders =
            merge_column_index_builders(column_index_builders, column_values);

        let mut bytes_to_write = 0;
        let mut column_index_offsets = HashMap::with_capacity(column_index_builders.len());
        for index_builder in column_index_builders {
            info!(
                "Column index builder: column {}, num_values {}",
                index_builder.column_name,
                index_builder.value_to_row_groups.len()
            );

            column_index_offsets.insert(index_builder.column_name.clone(), bytes_to_write as u64);
            let size = index_builder.write(&mut index_writer);
            bytes_to_write += size;
        }

        let column_indexes_json = serde_json::to_string(&column_index_offsets)
            .expect("Failed to serialize column indexes");
        let column_indexes_json_bytes = column_indexes_json.into_bytes();
        let column_indexes_json_bytes_size = column_indexes_json_bytes.len();
        index_writer
            .write(column_indexes_json_bytes)
            .expect("Failed to write column indexes");

        let offset_of_indexes = bytes_to_write as u64;
        let offset_of_indexes_bytes = offset_of_indexes.to_le_bytes().to_vec();
        let offset_of_indexes_bytes_size = 8;
        index_writer
            .write(offset_of_indexes_bytes)
            .expect("Failed to write offset of indexes");

        index_writer.close().expect("Failed to close index writer");
        info!(
            "Total written bytes {} for file {} to build index",
            bytes_to_write + column_indexes_json_bytes_size + offset_of_indexes_bytes_size,
            file_id
        );
    }
}

//                           ┌───────────────┐
//                          /│               │
//                         / │    bitmap_0   │
//                        /  │               │
//  ┌───────────────────┐/   ├───────────────┤
//  │                   │    │               │
//  │                   │    │    bitmap_1   │
//  │                   │    │               │
//  │                   │    └───────────────┘
//  │    col_index_0    │            .
//  │                   │            .
//  │                   │    ┌───────────────┐
//  │                   │    │               │
//  │                   │    │    bitmap_n   │
//  ├───────────────────┤    │               │
//  │                   │\   ├───────────────┤
//  │                   │ \  │               │
//  │                   │  \ │    fst map    │
//  │                   │   \│               │
//  │    col_index_1    │    └───────────────┘
//  │                   │
//  │                   │
//  │                   │
//  │                   │
//  └───────────────────┘
//           .
//           .
//  ┌───────────────────┐
//  │                   │
//  │                   │
//  │                   │
//  │                   │
//  │   col_index_n     │
//  │                   │
//  │                   │
//  │                   │
//  ├───────────────────┤
//  │                   │
//  │ offset_of_indexes │
//  │                   │
//  └───────────────────┘

struct ColumnIndexBuilder {
    column_name: String,
    value_to_row_groups: BTreeMap<Vec<u8>, roaring::bitmap::RoaringBitmap>,
}

impl ColumnIndexBuilder {
    fn write(&self, writer: &mut BlockingWriter) -> usize {
        let mut bytes_to_write = 0;
        let mut fst_builder = fst::MapBuilder::memory();
        for (value, row_groups) in &self.value_to_row_groups {
            fst_builder
                .insert(value, bytes_to_write as u64)
                .expect("Failed to insert value to fst");

            let size = row_groups.serialized_size();
            row_groups
                .serialize_into(&mut *writer)
                .expect("Failed to serialize row groups");

            bytes_to_write += size;
        }

        let fst_bytes = fst_builder
            .into_inner()
            .expect("Failed to get fst builder inner");
        let fst_size = fst_bytes.len();
        writer.write(fst_bytes).expect("Failed to write fst bytes");

        // write fst offset
        let offset_of_fst = bytes_to_write as u64;
        let offset_of_fst_bytes = offset_of_fst.to_le_bytes().to_vec();
        let offset_of_fst_bytes_size = 8;
        writer
            .write(offset_of_fst_bytes)
            .expect("Failed to write offset of indexes");

        info!(
            "Column {}, bitmaps size {}, FST size {}, total size {}",
            self.column_name,
            bytes_to_write,
            fst_size,
            bytes_to_write + fst_size + offset_of_fst_bytes_size
        );

        bytes_to_write += fst_size + offset_of_fst_bytes_size;
        bytes_to_write
    }
}

struct ColumnValues {
    row_group: usize,
    values: HashSet<Vec<u8>>,
}

fn column_values_to_row_group(
    region: MitoRegionRef,
    parquet_file_reader: Arc<dyn FileReader>,
    row_group: usize,
    pk_codec: McmpRowCodec,
) -> Vec<ColumnValues> {
    let mut column_index_builders = region
        .metadata()
        .primary_key_columns()
        .map(|_column| ColumnValues {
            row_group,
            values: HashSet::new(),
        })
        .collect::<Vec<_>>();

    let row_group_reader = parquet_file_reader
        .get_row_group(row_group)
        .expect("Failed to get row group reader");

    let num_columns = row_group_reader.num_columns();
    let num_rows = row_group_reader.metadata().num_rows();
    info!(
        "Row group info: row group {}, num_rows {}, num_columns {}",
        row_group, num_rows, num_columns
    );

    let schema_desc = row_group_reader.metadata().schema_descr_ptr();
    let pk_column_type = schema_desc
        .columns()
        .iter()
        .find_map(|c| {
            if c.name() == PRIMARY_KEY_COLUMN_NAME {
                Some(c.self_type_ptr())
            } else {
                None
            }
        })
        .expect("Failed to get pk column index");

    let root_type = schema_desc.root_schema();
    let root_basic_info = root_type.get_basic_info();
    let projection = GroupTypeBuilder::new(root_basic_info.name())
        .with_converted_type(root_basic_info.converted_type())
        .with_logical_type(root_basic_info.logical_type())
        .with_fields(&mut vec![pk_column_type])
        .build()
        .expect("Failed to build projection");

    let pk_row_iter = row_group_reader
        .get_row_iter(Some(projection))
        .expect("Failed to get pk row iter");

    for pk_row in pk_row_iter {
        let pk_row = pk_row.expect("Failed to get row");
        let (_, value) = pk_row
            .get_column_iter()
            .next()
            .expect("Failed to get column iter");

        let Field::Bytes(value) = value else {
            panic!("Failed to get pk value");
        };

        let pk_values = pk_codec
            .decode(value.data())
            .expect("Failed to decode pk value");

        for (value, index_builder) in pk_values.into_iter().zip(column_index_builders.iter_mut()) {
            let Value::String(sb) = value else {
                panic!("Unexpected pk value type")
            };

            index_builder
                .values
                .insert(sb.as_utf8().as_bytes().to_vec());
        }
    }

    column_index_builders
}

fn merge_column_index_builders(
    mut column_index_builders: Vec<ColumnIndexBuilder>,
    column_values: impl IntoIterator<Item = Vec<ColumnValues>>,
) -> Vec<ColumnIndexBuilder> {
    for column_values in column_values {
        for (
            ColumnValues {
                row_group, values, ..
            },
            ColumnIndexBuilder {
                value_to_row_groups,
                ..
            },
        ) in column_values
            .into_iter()
            .zip(column_index_builders.iter_mut())
        {
            for value in values {
                value_to_row_groups
                    .entry(value)
                    .or_default()
                    .insert(row_group as u32);
            }
        }
    }

    column_index_builders
}
