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
use std::sync::Arc;

use common_base::BitVec;
use common_query::Output;
use common_telemetry::info;
use datatypes::value::Value;
use fst::map::OpBuilder;
use fst::{IntoStreamer, Streamer};
use object_store::{BlockingWriter, Reader};
use parquet::file::reader::FileReader;
use parquet::record::Field;
use parquet::schema::types::GroupTypeBuilder;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use store_api::storage::RegionId;
use table::predicate::BytesPredicate;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

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
    value_to_row_groups: BTreeMap<Vec<u8>, BitVec>,
}

impl ColumnIndexBuilder {
    fn write(&self, writer: &mut BlockingWriter) -> usize {
        let mut bytes_to_write = 0;
        let mut fst_builder = fst::MapBuilder::memory();
        let writer = &mut *writer as &mut dyn std::io::Write;

        for (value, row_groups) in &self.value_to_row_groups {
            let size = row_groups.as_raw_slice().len();

            // encode offset as u32, size as u32, combine into u64
            let offset_and_size = ((bytes_to_write as u64) << 32) | (size as u64);
            fst_builder
                .insert(value, offset_and_size as u64)
                .expect("Failed to insert value to fst");

            writer
                .write(row_groups.as_raw_slice())
                .expect("Failed to write bitmap");

            bytes_to_write += size;
        }

        let fst_bytes = fst_builder
            .into_inner()
            .expect("Failed to get fst builder inner");
        let fst_size = fst_bytes.len();
        writer.write(&fst_bytes).expect("Failed to write fst bytes");

        // write fst size
        let fst_size_u64 = fst_size as u64;
        let fst_size_bytes = fst_size_u64.to_le_bytes().to_vec();
        let offset_of_fst_bytes_size = 8;
        writer
            .write(&fst_size_bytes)
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
                let bitvec = value_to_row_groups.entry(value).or_default();

                if bitvec.len() < row_group + 1 {
                    bitvec.resize(row_group + 1, false);
                }
                bitvec.set(row_group, true);
            }
        }
    }

    column_index_builders
}

pub struct ColumnIndexReaderBuilder {
    reader: Reader,
}

impl ColumnIndexReaderBuilder {
    pub fn new(reader: Reader) -> Self {
        Self { reader }
    }

    pub async fn build(mut self) -> ColumnIndexReader {
        let column_indices = self.read_column_indices().await;
        ColumnIndexReader::new(self.reader, column_indices)
    }

    async fn read_column_indices(&mut self) -> HashMap<String, (u64, u64)> {
        let offset_of_indexes_end = self
            .reader
            .seek(SeekFrom::End(-8))
            .await
            .expect("Failed to seek");
        let mut offset_of_indexes_bytes = [0; 8];
        self.reader
            .read_exact(&mut offset_of_indexes_bytes)
            .await
            .expect("Failed to read offset of indexes");
        let offset_of_indexes = u64::from_le_bytes(offset_of_indexes_bytes);
        self.reader
            .seek(SeekFrom::Start(offset_of_indexes))
            .await
            .expect("Failed to seek");

        let mut column_indexes_json_bytes =
            vec![0u8; (offset_of_indexes_end - offset_of_indexes) as usize];
        self.reader
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
    reader: Reader,
    column_indices_offsets: HashMap<String, (u64, u64)>,
}

impl ColumnIndexReader {
    fn new(reader: Reader, column_indices_offsets: HashMap<String, (u64, u64)>) -> Self {
        Self {
            reader,
            column_indices_offsets,
        }
    }

    pub async fn read_column_index(
        &mut self,
        column_name: &str,
    ) -> Option<InvertedValuesReader<'_>> {
        let Some((offset_begin, offset_end)) = self.column_indices_offsets.get(column_name) else {
            return None;
        };

        self.reader
            .seek(SeekFrom::Start(offset_end - 8))
            .await
            .expect("Failed to seek");

        let mut fst_size_bytes = [0; 8];
        self.reader
            .read_exact(&mut fst_size_bytes)
            .await
            .expect("Failed to read offset of fst");
        let fst_size = u64::from_le_bytes(fst_size_bytes);

        self.reader
            .seek(SeekFrom::Start(offset_end - fst_size - 8))
            .await
            .expect("Failed to seek");

        let mut fst_bytes = vec![0u8; fst_size as usize];
        self.reader
            .read_exact(&mut fst_bytes)
            .await
            .expect("Failed to read fst bytes");

        let fst = fst::Map::new(fst_bytes).expect("Failed to get fst");

        Some(InvertedValuesReader {
            reader: &mut self.reader,
            offset_begin: *offset_begin,
            fst,
        })
    }
}

pub struct InvertedValuesReader<'a> {
    reader: &'a mut Reader,
    offset_begin: u64,
    fst: fst::Map<Vec<u8>>,
}

impl<'a> InvertedValuesReader<'a> {
    pub fn apply_predicates_to_offsets(&mut self, predicates: &[BytesPredicate]) -> Vec<u64> {
        let mut op = OpBuilder::new();
        for predicate in predicates {
            match predicate {
                BytesPredicate::Eq(bytes) => {
                    return match self.fst.get(bytes) {
                        Some(offset_size) => vec![offset_size],
                        None => vec![],
                    };
                }

                BytesPredicate::InList(bytes) => {
                    let mut bitmap_offset_sizes = vec![];
                    for bytes in bytes {
                        if let Some(offset_size) = self.fst.get(bytes) {
                            bitmap_offset_sizes.push(offset_size);
                        }
                    }
                    return bitmap_offset_sizes;
                }

                BytesPredicate::Gt(bytes) => {
                    op.push(self.fst.range().gt(bytes.as_slice()));
                }
                BytesPredicate::GtEq(bytes) => {
                    op.push(self.fst.range().ge(bytes.as_slice()));
                }
                BytesPredicate::Lt(bytes) => {
                    op.push(self.fst.range().lt(bytes.as_slice()));
                }
                BytesPredicate::LtEq(bytes) => {
                    op.push(self.fst.range().le(bytes.as_slice()));
                }
                BytesPredicate::InRangeInclusive(left, right) => {
                    op.push(self.fst.range().le(right.as_slice()).ge(left.as_slice()));
                }
                _ => {}
            }
        }

        let mut intersection = op.intersection().into_stream();
        let mut bitmap_offset_sizes = vec![];
        while let Some((_, indexed_values)) = intersection.next() {
            bitmap_offset_sizes.push(indexed_values[0].value);
        }

        bitmap_offset_sizes
    }

    pub async fn apply(&mut self, predicates: &[BytesPredicate], mut row_groups: BitVec) -> BitVec {
        let bitmap_offset_sizes = self.apply_predicates_to_offsets(predicates);
        for bitmap_offset_size in bitmap_offset_sizes {
            let offset = (bitmap_offset_size >> 32) as u64;
            let size = (bitmap_offset_size & 0xffffffff) as usize;
            self.reader
                .seek(SeekFrom::Start(self.offset_begin + offset))
                .await
                .expect("Failed to seek");
            let mut bitmap_bytes = vec![0u8; size];
            self.reader
                .read_exact(&mut bitmap_bytes)
                .await
                .expect("Failed to read bitmap bytes");

            let read_bitvec =
                BitVec::try_from_slice(bitmap_bytes.as_slice()).expect("Failed to deserialize");
            row_groups |= read_bitvec;
        }

        row_groups
    }
}
