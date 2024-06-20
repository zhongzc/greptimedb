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

use std::collections::HashSet;
use std::path::PathBuf;

use async_compression::futures::bufread::{ZstdDecoder, ZstdEncoder};
use async_trait::async_trait;
use async_walkdir::{Filtering, WalkDir};
use futures::io::{copy, BufReader};
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncWrite, StreamExt};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use tokio_util::compat::TokioAsyncReadCompatExt;
use uuid::Uuid;

use crate::blob_metadata::CompressionCodec;
use crate::error::{
    BlobIndexOutOfBoundSnafu, BlobNotFoundSnafu, CopySnafu, DeserializeJsonSnafu,
    DuplicateBlobSnafu, FileKeyNotMatchSnafu, MetadataSnafu, OpenSnafu, ReadSnafu, Result,
    SerializeJsonSnafu, UnsupportedCompressionSnafu, UnsupportedDecompressionSnafu, WalkDirSnafu,
};
use crate::file_format::reader::{PuffinAsyncReader, PuffinFileReader};
use crate::file_format::writer::{Blob, PuffinAsyncWriter, PuffinFileWriter};
use crate::puffin_manager::cache_manager::CacheManagerRef;
use crate::puffin_manager::file_accessor::PuffinFileAccessorRef;
use crate::puffin_manager::{PuffinManager, PuffinReader, PuffinWriter, PutOptions};

pub struct CachedPuffinManager<CR, AR, AW> {
    cache_manager: CacheManagerRef<CR>,
    puffin_file_accessor: PuffinFileAccessorRef<AR, AW>,
}

impl<CR, AR, AW> CachedPuffinManager<CR, AR, AW> {
    pub fn new(
        cache_manager: CacheManagerRef<CR>,
        puffin_file_accessor: PuffinFileAccessorRef<AR, AW>,
    ) -> Self {
        Self {
            cache_manager,
            puffin_file_accessor,
        }
    }
}

#[async_trait]
impl<CR, AR, AW> PuffinManager for CachedPuffinManager<CR, AR, AW>
where
    CR: AsyncRead + AsyncSeek,
    AR: AsyncRead + AsyncSeek + Send + Unpin + 'static,
    AW: AsyncWrite + Send + Unpin + 'static,
{
    type Reader = CachedPuffinReader<CR, AR, AW>;
    type Writer = CachedPuffinWriter<CR, AW>;

    async fn reader(&self, puffin_file_name: &str) -> Result<Self::Reader> {
        Ok(CachedPuffinReader {
            puffin_file_name: puffin_file_name.to_string(),
            cache_manager: self.cache_manager.clone(),
            puffin_file_accessor: self.puffin_file_accessor.clone(),
        })
    }

    async fn writer(&self, puffin_file_name: &str) -> Result<Self::Writer> {
        let writer = self.puffin_file_accessor.writer(puffin_file_name).await?;
        Ok(CachedPuffinWriter {
            puffin_file_name: puffin_file_name.to_string(),
            cache_manager: self.cache_manager.clone(),
            puffin_file_writer: PuffinFileWriter::new(writer),
            blob_keys: Default::default(),
        })
    }
}

pub struct CachedPuffinReader<CR, AR, AW> {
    puffin_file_name: String,
    cache_manager: CacheManagerRef<CR>,
    puffin_file_accessor: PuffinFileAccessorRef<AR, AW>,
}

#[async_trait]
impl<CR, AR, AW> PuffinReader for CachedPuffinReader<CR, AR, AW>
where
    AR: AsyncRead + AsyncSeek + Send + Unpin + 'static,
    AW: AsyncWrite + 'static,
    CR: AsyncRead + AsyncSeek,
{
    type Reader = CR;

    async fn blob(&self, key: &str) -> Result<Self::Reader> {
        self.cache_manager
            .get_blob(
                self.puffin_file_name.as_str(),
                key,
                Box::new(move |mut writer| {
                    let accessor = self.puffin_file_accessor.clone();
                    let puffin_file_name = self.puffin_file_name.clone();
                    let key = key.to_string();
                    Box::pin(async move {
                        let reader = accessor.reader(&puffin_file_name).await?;
                        let mut file = PuffinFileReader::new(reader);

                        let metadata = file.metadata().await?;
                        let blob_metadata = metadata
                            .blobs
                            .iter()
                            .find(|m| m.blob_type == key.as_str())
                            .context(BlobNotFoundSnafu { blob: key })?;
                        let reader = file.blob_reader(blob_metadata)?;

                        let size = match blob_metadata.compression_codec {
                            Some(CompressionCodec::Lz4) => {
                                return UnsupportedDecompressionSnafu { codec: "lz4" }.fail();
                            }
                            Some(CompressionCodec::Zstd) => {
                                let reader = ZstdDecoder::new(BufReader::new(reader));
                                copy(reader, &mut writer).await.context(CopySnafu)?
                            }
                            None => copy(reader, &mut writer).await.context(CopySnafu)?,
                        };

                        Ok(size)
                    })
                }),
            )
            .await
    }

    async fn dir(&self, key: &str) -> Result<PathBuf> {
        self.cache_manager
            .get_dir(
                self.puffin_file_name.as_str(),
                key,
                Box::new(|mut writer_provider| {
                    let accessor = self.puffin_file_accessor.clone();
                    let puffin_file_name = self.puffin_file_name.clone();
                    let key = key.to_string();
                    Box::pin(async move {
                        let reader = accessor.reader(&puffin_file_name).await?;
                        let mut file = PuffinFileReader::new(reader);

                        let puffin_metadata = file.metadata().await?;
                        let blob_metadata = puffin_metadata
                            .blobs
                            .iter()
                            .find(|m| m.blob_type == key.as_str())
                            .context(BlobNotFoundSnafu { blob: key })?;
                        let mut reader = file.blob_reader(blob_metadata)?;

                        let mut buf = vec![];
                        reader.read_to_end(&mut buf).await.context(ReadSnafu)?;

                        let dir_meta: DirMetadata =
                            serde_json::from_slice(buf.as_slice()).context(DeserializeJsonSnafu)?;

                        let mut size = 0;
                        for file_meta in dir_meta.files {
                            let blob_meta = puffin_metadata
                                .blobs
                                .get(file_meta.blob_index)
                                .with_context(|| BlobIndexOutOfBoundSnafu {
                                    index: file_meta.blob_index,
                                    max_index: puffin_metadata.blobs.len(),
                                })?;
                            ensure!(
                                blob_meta.blob_type == file_meta.key,
                                FileKeyNotMatchSnafu {
                                    expected: file_meta.key,
                                    actual: &blob_meta.blob_type,
                                }
                            );

                            let reader = file.blob_reader(blob_meta)?;

                            let mut writer = writer_provider(file_meta.file_rel_path).await?;
                            match blob_meta.compression_codec {
                                Some(CompressionCodec::Lz4) => {
                                    UnsupportedDecompressionSnafu { codec: "lz4" }.fail()?;
                                }
                                Some(CompressionCodec::Zstd) => {
                                    let reader = ZstdDecoder::new(BufReader::new(reader));
                                    size += copy(reader, &mut writer).await.context(CopySnafu)?;
                                }
                                None => {
                                    size += copy(reader, &mut writer).await.context(CopySnafu)?;
                                }
                            }
                        }

                        Ok(size)
                    })
                }),
            )
            .await
    }
}

pub struct CachedPuffinWriter<CR, W> {
    puffin_file_name: String,
    cache_manager: CacheManagerRef<CR>,
    puffin_file_writer: PuffinFileWriter<W>,
    blob_keys: HashSet<String>,
}

#[async_trait]
impl<CR, W> PuffinWriter for CachedPuffinWriter<CR, W>
where
    CR: AsyncRead + AsyncSeek,
    W: AsyncWrite + Unpin + Send,
{
    async fn put_blob(
        &mut self,
        key: &str,
        raw_data: impl AsyncRead + Send,
        options: Option<PutOptions>,
    ) -> Result<()> {
        ensure!(
            !self.blob_keys.contains(key),
            DuplicateBlobSnafu { blob: key }
        );

        let compression_codec = options.and_then(|opts| opts.data_compression);
        match compression_codec {
            Some(CompressionCodec::Lz4) => UnsupportedCompressionSnafu { codec: "lz4" }.fail()?,
            Some(CompressionCodec::Zstd) => {
                let blob = Blob {
                    blob_type: key.to_string(),
                    compressed_data: ZstdEncoder::new(BufReader::new(raw_data)),
                    compression_codec,
                    properties: Default::default(),
                };
                self.puffin_file_writer.add_blob(blob).await?;
            }
            None => {
                let blob = Blob {
                    blob_type: key.to_string(),
                    compressed_data: raw_data,
                    compression_codec,
                    properties: Default::default(),
                };
                self.puffin_file_writer.add_blob(blob).await?;
            }
        }

        self.blob_keys.insert(key.to_string());
        Ok(())
    }

    async fn put_dir(
        &mut self,
        key: &str,
        dir_path: PathBuf,
        options: Option<PutOptions>,
    ) -> Result<()> {
        ensure!(
            !self.blob_keys.contains(key),
            DuplicateBlobSnafu { blob: key }
        );

        let compression_codec = options.and_then(|opts| opts.data_compression);
        ensure!(
            !matches!(compression_codec, Some(CompressionCodec::Lz4)),
            UnsupportedCompressionSnafu { codec: "lz4" }
        );

        let mut files = vec![];

        let mut wd = WalkDir::new(&dir_path).filter(|entry| async move {
            match entry.file_type().await {
                Ok(ft) if ft.is_dir() => Filtering::Ignore,
                _ => Filtering::Continue,
            }
        });

        let mut size = 0;
        while let Some(entry) = wd.next().await {
            let entry = entry.context(WalkDirSnafu)?;
            size += entry.metadata().await.context(MetadataSnafu)?.len();

            let reader = tokio::fs::File::open(entry.path())
                .await
                .context(OpenSnafu)?
                .compat();

            let key = Uuid::new_v4().to_string();
            match compression_codec {
                Some(CompressionCodec::Lz4) => unreachable!("checked above"),
                Some(CompressionCodec::Zstd) => {
                    let blob = Blob {
                        blob_type: key.clone(),
                        compressed_data: ZstdEncoder::new(BufReader::new(reader)),
                        compression_codec: compression_codec.clone(),
                        properties: Default::default(),
                    };
                    self.puffin_file_writer.add_blob(blob).await?;
                }
                None => {
                    let blob = Blob {
                        blob_type: key.clone(),
                        compressed_data: reader,
                        compression_codec: compression_codec.clone(),
                        properties: Default::default(),
                    };
                    self.puffin_file_writer.add_blob(blob).await?;
                }
            }

            let file_rel_path = entry
                .path()
                .strip_prefix(&dir_path)
                .expect("entry path is under dir path")
                .to_string_lossy()
                .into_owned();

            files.push(DirFileMetadata {
                file_rel_path,
                key: key.clone(),
                blob_index: self.blob_keys.len(),
            });
            self.blob_keys.insert(key);
        }

        let dir_metadata = DirMetadata { files };
        let encoded = serde_json::to_vec(&dir_metadata).context(SerializeJsonSnafu)?;
        let dir_meta_blob = Blob {
            blob_type: key.to_string(),
            compressed_data: encoded.as_slice(),
            compression_codec: None,
            properties: Default::default(),
        };

        self.puffin_file_writer.add_blob(dir_meta_blob).await?;
        self.blob_keys.insert(key.to_string());

        self.cache_manager
            .put_dir(&self.puffin_file_name, key, dir_path, size)
            .await?;
        Ok(())
    }

    fn set_footer_lz4_compressed(&mut self, lz4_compressed: bool) {
        self.puffin_file_writer
            .set_footer_lz4_compressed(lz4_compressed);
    }

    async fn finish(mut self) -> Result<()> {
        self.puffin_file_writer.finish().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirMetadata {
    pub files: Vec<DirFileMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirFileMetadata {
    pub file_rel_path: String,
    pub key: String,
    pub blob_index: usize,
}
