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

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use object_store::util::join_path;
use object_store::{util, BlockingReader, BlockingWriter, ObjectStore};
use parquet::file::reader::{ChunkReader, FileReader, Length};
use parquet::file::serialized_reader::SerializedFileReader;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;

use crate::error::{DeleteSstSnafu, Result};
use crate::read::Source;
use crate::sst::file::{FileHandle, FileId};
use crate::sst::parquet::reader::ParquetReaderBuilder;
use crate::sst::parquet::writer::ParquetWriter;

pub type AccessLayerRef = Arc<AccessLayer>;

/// A layer to access SST files under the same directory.
pub struct AccessLayer {
    region_dir: String,
    object_store: ObjectStore,
}

impl std::fmt::Debug for AccessLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessLayer")
            .field("region_dir", &self.region_dir)
            .finish()
    }
}

impl AccessLayer {
    /// Returns a new [AccessLayer] for specific `region_dir`.
    pub fn new(region_dir: impl Into<String>, object_store: ObjectStore) -> AccessLayer {
        AccessLayer {
            region_dir: region_dir.into(),
            object_store,
        }
    }

    /// Returns the directory of the region.
    pub fn region_dir(&self) -> &str {
        &self.region_dir
    }

    /// Returns the object store of the layer.
    pub fn object_store(&self) -> &ObjectStore {
        &self.object_store
    }

    /// Deletes a SST file with given file id.
    pub(crate) async fn delete_sst(&self, file_id: FileId) -> Result<()> {
        let path = self.sst_file_path(&file_id.as_parquet());
        self.object_store
            .delete(&path)
            .await
            .context(DeleteSstSnafu { file_id })
    }

    /// Returns a reader builder for specific `file`.
    pub(crate) fn read_sst(&self, file: FileHandle) -> ParquetReaderBuilder {
        ParquetReaderBuilder::new(self.region_dir.clone(), file, self.object_store.clone())
    }

    pub(crate) fn sst_file_reader(&self, file: FileHandle) -> Arc<dyn FileReader> {
        let file_path = file.file_path(&self.region_dir);
        let chunk_reader = ChunkReaderAdapter {
            object_store: self.object_store.clone(),
            file_path,
        };
        let file_reader =
            SerializedFileReader::new(chunk_reader).expect("Failed to create file reader");
        Arc::new(file_reader)
    }

    //
    pub(crate) fn new_index_writer(&self, file_id: &FileId) -> Option<BlockingWriter> {
        let file_path = join_path(&self.region_dir, &format!("{}.index", file_id));
        if self
            .object_store
            .blocking()
            .is_exist(&file_path)
            .expect("Failed to check index file existence")
        {
            None
        } else {
            Some(
                self.object_store
                    .blocking()
                    .writer(&file_path)
                    .expect("Failed to create index writer"),
            )
        }
    }

    /// Returns a new parquet writer to write the SST for specific `file_id`.
    // TODO(hl): maybe rename to [sst_writer].
    pub(crate) fn write_sst(
        &self,
        file_id: FileId,
        metadata: RegionMetadataRef,
        source: Source,
    ) -> ParquetWriter {
        let path = self.sst_file_path(&file_id.as_parquet());
        ParquetWriter::new(path, metadata, source, self.object_store.clone())
    }

    /// Returns the `file_path` for the `file_name` in the object store.
    fn sst_file_path(&self, file_name: &str) -> String {
        util::join_path(&self.region_dir, file_name)
    }
}

struct ChunkReaderAdapter {
    object_store: ObjectStore,
    file_path: String,
}

impl ChunkReader for ChunkReaderAdapter {
    type T = BlockingReader;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut reader = self
            .object_store
            .blocking()
            .reader(&self.file_path)
            .expect("Failed to create reader");
        reader.seek(SeekFrom::Start(start)).expect("Failed to seek");
        Ok(reader)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        let mut reader = self
            .object_store
            .blocking()
            .reader(&self.file_path)
            .expect("Failed to create reader");
        reader.seek(SeekFrom::Start(start)).expect("Failed to seek");
        let mut buffer = Vec::with_capacity(length);
        let read = std::io::Read::take(reader, length as _)
            .read_to_end(&mut buffer)
            .expect("Failed to read");

        if read != length {
            return Err(parquet::errors::ParquetError::General(format!(
                "Expected to read {} bytes, read only {}",
                length, read
            )));
        }

        Ok(buffer.into())
    }
}

impl Length for ChunkReaderAdapter {
    fn len(&self) -> u64 {
        let metadata = self
            .object_store
            .blocking()
            .stat(&self.file_path)
            .expect("Failed to get metadata");
        metadata.content_length()
    }
}
