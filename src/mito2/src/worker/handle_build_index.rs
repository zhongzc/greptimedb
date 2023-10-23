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
use std::io::SeekFrom;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use common_query::Output;
use common_telemetry::info;
use datatypes::value::Value;
use fst::{IntoStreamer, Streamer};
use object_store::{BlockingWriter, Reader};
use parquet::file::reader::FileReader;
use parquet::record::Field;
use parquet::schema::types::GroupTypeBuilder;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use store_api::storage::RegionId;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::SyncIoBridge;

use crate::error::Result;
use crate::region::MitoRegionRef;
use crate::request::OptionOutputTx;
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};
use crate::sst::file::FileId;
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

        // write fst size
        let fst_size_u64 = fst_size as u64;
        let fst_size_bytes = fst_size_u64.to_le_bytes().to_vec();
        let offset_of_fst_bytes_size = 8;
        writer
            .write(fst_size_bytes)
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

pub struct ColumnIndexReaderBuilder {}

impl ColumnIndexReaderBuilder {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn build(self, mut reader: Reader) -> (ColumnIndexReader, Reader) {
        let column_indices = self.read_column_indices(&mut reader).await;
        (ColumnIndexReader::new(column_indices), reader)
    }

    async fn read_column_indices(&self, reader: &mut Reader) -> HashMap<String, (u64, u64)> {
        let offset_of_indexes_end = reader
            .seek(SeekFrom::End(-8))
            .await
            .expect("Failed to seek");
        let mut offset_of_indexes_bytes = [0; 8];
        reader
            .read_exact(&mut offset_of_indexes_bytes)
            .await
            .expect("Failed to read offset of indexes");
        let offset_of_indexes = u64::from_le_bytes(offset_of_indexes_bytes);
        reader
            .seek(SeekFrom::Start(offset_of_indexes))
            .await
            .expect("Failed to seek");

        let mut column_indexes_json_bytes =
            vec![0u8; (offset_of_indexes_end - offset_of_indexes) as usize];
        reader
            .read_exact(&mut column_indexes_json_bytes)
            .await
            .expect("Failed to read column indexes json bytes");
        let js: HashMap<String, u64> = serde_json::from_slice(&column_indexes_json_bytes)
            .expect("Failed to deserialize column indexes json");

        // column->offset begin ---> column->(offset begin, end begin)
        let mut s = js.into_iter().collect::<Vec<(String, u64)>>();
        s.sort_by(|a, b| a.1.cmp(&b.1));
        let mut column_offsets = HashMap::with_capacity(s.len());
        for (i, (column, offset_begin)) in s.iter().enumerate() {
            let offset_end = if i == s.len() - 1 {
                offset_of_indexes
            } else {
                s[i + 1].1
            };
            column_offsets.insert(column.clone(), (*offset_begin, offset_end));
        }

        column_offsets
    }
}

pub struct ColumnIndexReader {
    column_indices_offsets: HashMap<String, (u64, u64)>,
}

impl ColumnIndexReader {
    fn new(column_indices_offsets: HashMap<String, (u64, u64)>) -> Self {
        Self {
            column_indices_offsets,
        }
    }

    pub async fn read_column_index(
        &self,
        column_name: &str,
        mut reader: Reader,
    ) -> (Option<InvertedValuesReader>, Reader) {
        let Some((offset_begin, offset_end)) = self.column_indices_offsets.get(column_name) else {
            return (None, reader);
        };

        reader
            .seek(SeekFrom::Start(offset_end - 8))
            .await
            .expect("Failed to seek");

        let mut fst_size_bytes = [0; 8];
        reader
            .read_exact(&mut fst_size_bytes)
            .await
            .expect("Failed to read offset of fst");
        let fst_size = u64::from_le_bytes(fst_size_bytes);

        reader
            .seek(SeekFrom::Start(offset_end - fst_size - 8))
            .await
            .expect("Failed to seek");

        let mut fst_bytes = vec![0u8; fst_size as usize];
        reader
            .read_exact(&mut fst_bytes)
            .await
            .expect("Failed to read fst bytes");

        let fst = fst::Map::new(fst_bytes).expect("Failed to get fst");

        (
            Some(InvertedValuesReader {
                offset_begin: *offset_begin,
                fst,
            }),
            reader,
        )
    }
}

pub struct InvertedValuesReader {
    offset_begin: u64,
    fst: fst::Map<Vec<u8>>,
}

impl InvertedValuesReader {
    pub async fn union(
        &self,
        bound: impl RangeBounds<&[u8]>,
        mut reader: Reader,
    ) -> (roaring::bitmap::RoaringBitmap, Reader) {
        let mut bitmap = roaring::RoaringBitmap::new();
        let fst_range = self.fst.range();
        let fst_range = match bound.start_bound() {
            Bound::Included(v) => fst_range.ge(v),
            Bound::Excluded(v) => fst_range.gt(v),
            Bound::Unbounded => fst_range,
        };
        let fst_range = match bound.end_bound() {
            Bound::Included(v) => fst_range.le(v),
            Bound::Excluded(v) => fst_range.lt(v),
            Bound::Unbounded => fst_range,
        };

        let mut bitmap_offsets = fst_range.into_stream();
        let Some((_, first_bitmap_offset)) = bitmap_offsets.next() else {
            return (bitmap, reader);
        };

        reader
            .seek(SeekFrom::Start(self.offset_begin + first_bitmap_offset))
            .await
            .expect("Failed to seek");

        let mut bitmap_count = 1;
        while let Some(_) = bitmap_offsets.next() {
            bitmap_count += 1;
        }
        tokio::task::spawn_blocking(move || {
            let mut blocking_reader = SyncIoBridge::new(&mut reader);
            for _ in 0..bitmap_count {
                bitmap |= roaring::RoaringBitmap::deserialize_from(&mut blocking_reader)
                    .expect("Failed to deserialize");
            }

            (bitmap, reader)
        })
        .await
        .expect("Failed to join blocking task")
    }
}
