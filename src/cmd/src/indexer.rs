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

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use common_config::KvBackendConfig;
use common_runtime::Runtime;
use common_telemetry::logging::LoggingOptions;
use common_telemetry::{info, warn};
use common_wal::config::StandaloneWalConfig;
use datanode::config::{
    DatanodeOptions, FileConfig, ProcedureConfig, RegionEngineConfig, StorageConfig,
};
use datanode::store::fs::new_fs_object_store;
use file_engine::config::EngineConfig as FileEngineConfig;
use frontend::frontend::FrontendOptions;
use frontend::service_config::{
    GrpcOptions, InfluxdbOptions, MysqlOptions, OpentsdbOptions, PostgresOptions, PromStoreOptions,
};
use itertools::Itertools;
use mito2::config::MitoConfig;
use mito2::region::options::{IndexOptions, InvertedIndexOptions};
use mito2::sst::file;
use mito2::sst::index::intermediate::IntermediateManager;
use mito2::sst::index::IndexerBuilder;
use mito2::sst::parquet::format::ReadFormat;
use mito2::sst::parquet::metadata::MetadataLoader;
use mito2::sst::parquet::row_group::InMemoryRowGroup;
use mito2::sst::parquet::{DEFAULT_ROW_GROUP_SIZE, PARQUET_METADATA_KEY};
use object_store::util::{join_dir, join_path};
use object_store::{Entry, EntryMode, Metakey, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::{parquet_to_arrow_field_levels, ProjectionMask};
use serde::{Deserialize, Serialize};
use servers::export_metrics::ExportMetricsOption;
use servers::http::HttpOptions;
use servers::Mode;
use store_api::metadata::RegionMetadata;
use store_api::storage::RegionId;
use tokio::sync::Mutex;

use crate::error::Result;
use crate::options::{CliOptions, MixOptions, Options};
use crate::App;

#[derive(Parser)]
pub struct Command {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

impl Command {
    pub async fn build(self, opts: MixOptions) -> Result<Instance> {
        self.subcmd.build(opts).await
    }

    pub fn load_options(&self, cli_options: &CliOptions) -> Result<Options> {
        self.subcmd.load_options(cli_options)
    }
}

type FileId = String;

#[derive(Clone)]
pub struct Instance {
    data_store: ObjectStore,
    intm: IntermediateManager,
    state: Arc<Mutex<HashMap<FileId, DataInfo>>>,
    rt: Runtime,
}

struct DataInfo {
    file_id: FileId,
    size: u64,
    index_state: IndexState,
}

impl fmt::Display for DataInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[file_id: {}, size: {}, index_state: {:?}]",
            self.file_id, self.size, self.index_state
        )
    }
}

#[derive(Debug, PartialEq, Eq)]
enum IndexState {
    NotFound,
    Indexing,
    Indexed,
}

impl Instance {
    async fn list_data(&self) -> HashMap<FileId, Entry> {
        self.data_store
            .list_with("data/greptime/public/1024/1024_0000000000/data/")
            .metakey(Metakey::ContentLength)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| {
                matches!(e.metadata().mode(), EntryMode::FILE) && e.name().ends_with(".parquet")
            })
            .map(|e| {
                let file_id = e.name().trim_end_matches(".parquet").to_string();
                (file_id, e)
            })
            .collect()
    }

    async fn list_index(&self) -> HashMap<FileId, Entry> {
        self.data_store
            .list("data/greptime/public/1024/1024_0000000000/data/index/")
            .await
            .unwrap()
            .into_iter()
            .filter(|e| {
                matches!(e.metadata().mode(), EntryMode::FILE) && e.name().ends_with(".puffin")
            })
            .map(|e| {
                let file_id = e.name().trim_end_matches(".puffin").to_string();
                (file_id, e)
            })
            .collect()
    }

    async fn init_info(&self) -> Result<()> {
        let mut state = self.state.lock().await;

        for (file_id, entry) in self.list_data().await {
            let size = entry.metadata().content_length();
            state.insert(
                file_id.clone(),
                DataInfo {
                    file_id,
                    size,
                    index_state: IndexState::NotFound,
                },
            );
        }

        for (file_id, entry) in self.list_index().await {
            if let Some(data_info) = state.get_mut(&file_id) {
                data_info.index_state = IndexState::Indexed;
            } else {
                // remove outdated index
                self.data_store.delete(entry.path()).await.unwrap();
            }
        }

        let state = state.values().map(|info| info.to_string()).join(", ");
        info!("State [greptime/public/1024/1024_0000000000]: {}", state);

        Ok(())
    }
}

#[async_trait]
impl App for Instance {
    fn name(&self) -> &str {
        "greptime-indexer"
    }

    async fn start(&mut self) -> Result<()> {
        self.init_info().await?;

        let instance = self.clone();
        let rt = self.rt.clone();
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;

                let data = instance.list_data().await;

                let mut state = instance.state.lock().await;
                for (file_id, entry) in &data {
                    if state.contains_key(file_id) {
                        continue;
                    }

                    info!(
                        "New data file [greptime/public/1024/1024_0000000000]: {}",
                        file_id
                    );

                    let size = entry.metadata().content_length();
                    state.insert(
                        file_id.clone(),
                        DataInfo {
                            file_id: file_id.clone(),
                            size,
                            index_state: IndexState::NotFound,
                        },
                    );
                }

                let removed_files = state
                    .keys()
                    .filter(|file_id| !data.contains_key(*file_id))
                    .cloned()
                    .collect::<Vec<_>>();

                for file_id in removed_files {
                    let info = state.remove(&file_id).unwrap();
                    if info.index_state == IndexState::Indexed {
                        let index_path = join_dir(
                            "data/greptime/public/1024/1024_0000000000/data/index/",
                            &format!("{}.puffin", file_id),
                        );
                        instance.data_store.delete(&index_path).await.unwrap();
                        info!(
                            "Removed index file [greptime/public/1024/1024_0000000000]: {}",
                            file_id
                        );
                    }
                }

                for (file_id, info) in state.iter_mut() {
                    if info.index_state == IndexState::NotFound {
                        info.index_state = IndexState::Indexing;
                        let file_path = join_path(
                            "data/greptime/public/1024/1024_0000000000/data/",
                            &format!("{}.parquet", file_id),
                        );
                        let index_path = join_path(
                            "data/greptime/public/1024/1024_0000000000/data/index/",
                            &format!("{}.puffin", file_id),
                        );

                        let file_id_str = file_id.clone();
                        let file_size = info.size;
                        let intm = instance.intm.clone();
                        let state = instance.state.clone();
                        let object_store = instance.data_store.clone();
                        info!("Start indexing [greptime/public/1024/1024_0000000000]: file_id: {file_id}");
                        rt.spawn(async move {
                            let loader =
                                MetadataLoader::new(object_store.clone(), &file_path, file_size);
                            let metadata = loader.load().await.unwrap();
                            let key_value_meta =
                                metadata.file_metadata().key_value_metadata().unwrap();
                            let meta_value = key_value_meta
                                .iter()
                                .find(|kv| kv.key == PARQUET_METADATA_KEY)
                                .unwrap()
                                .value
                                .as_ref()
                                .unwrap();

                            let region_metadata =
                                Arc::new(RegionMetadata::from_json(meta_value).unwrap());

                            let file_id = file::FileId::parse_str(&file_id_str).unwrap();
                            let mut indexer = IndexerBuilder {
                                create_inverted_index: true,
                                mem_threshold_index_create: None,
                                write_buffer_size: None,
                                file_id,
                                file_path: index_path.clone(),
                                metadata: &region_metadata,
                                row_group_size: DEFAULT_ROW_GROUP_SIZE,
                                object_store: object_store.clone(),
                                intermediate_manager: intm,
                                index_options: IndexOptions {
                                    inverted_index: InvertedIndexOptions {
                                        ignore_column_ids: vec![2147483651],
                                        segment_row_count: 256,
                                    },
                                },
                            }
                            .build();

                            let desc = metadata.file_metadata().schema_descr();
                            let projection_mask = ProjectionMask::roots(
                                desc,
                                [
                                    desc.num_columns() - 4,
                                    desc.num_columns() - 3,
                                    desc.num_columns() - 2,
                                    desc.num_columns() - 1,
                                ],
                            );

                            let mut read_format = ReadFormat::new(region_metadata.clone());
                            let hint = Some(read_format.arrow_schema().fields());
                            let field_levels =
                                parquet_to_arrow_field_levels(desc, projection_mask.clone(), hint)
                                    .unwrap();

                            for row_group_idx in 0..metadata.row_groups().len() {
                                let mut row_group = InMemoryRowGroup::create(
                                    RegionId::from_u64(1024),
                                    file_id,
                                    &metadata,
                                    row_group_idx,
                                    None,
                                    &file_path,
                                    object_store.clone(),
                                );
                                row_group.fetch(&projection_mask, None).await.unwrap();

                                let mut reader = ParquetRecordBatchReader::try_new_with_row_groups(
                                    &field_levels,
                                    &row_group,
                                    1024,
                                    None,
                                )
                                .unwrap();

                                let mut batches = VecDeque::new();
                                while let Some(batch) = reader.next() {
                                    let batch = batch.unwrap();

                                    read_format
                                        .convert_record_batch(&batch, &mut batches)
                                        .unwrap();

                                    for batch in batches.drain(..) {
                                        indexer.update(&batch).await;
                                    }
                                }
                            }

                            let size = indexer.finish().await;
                            match size {
                                Some(size) => {
                                    info!("Indexing finished [greptime/public/1024/1024_0000000000]: file_id: {file_id}, file_size: {size}");
                                }
                                _ => {
                                    warn!("Failed to index [greptime/public/1024/1024_0000000000]: file_id: {file_id}");
                                }
                            }

                            let mut state = state.lock().await;
                            match state.get_mut(&file_id_str) {
                                Some(info) => {
                                    info.index_state = IndexState::Indexed;
                                }
                                _ => {
                                    warn!("File not found [greptime/public/1024/1024_0000000000], delete it: file_id: {file_id}");
                                    object_store.delete(&index_path).await.unwrap();
                                }
                            }
                        });
                    }
                }
            }
        });

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!("Indexer instance stopped.");

        Ok(())
    }
}

#[derive(Parser)]
enum SubCommand {
    Start(StartCommand),
}

impl SubCommand {
    async fn build(self, opts: MixOptions) -> Result<Instance> {
        match self {
            SubCommand::Start(cmd) => cmd.build(opts).await,
        }
    }

    fn load_options(&self, cli_options: &CliOptions) -> Result<Options> {
        match self {
            SubCommand::Start(cmd) => cmd.load_options(cli_options),
        }
    }
}

#[derive(Debug, Default, Parser)]
pub struct StartCommand {
    #[clap(short, long)]
    pub config_file: Option<String>,
    #[clap(long, default_value = "GREPTIMEDB_STANDALONE")]
    pub env_prefix: String,
}

impl StartCommand {
    async fn build(self, opts: MixOptions) -> Result<Instance> {
        info!("Indexer start command: {:#?}", self);
        info!("Building indexer instance with {opts:#?}");

        let intm = IntermediateManager::init_fs(&format!("{}/index_intermediate", opts.data_home))
            .await
            .unwrap();

        let data_store =
            new_fs_object_store(&opts.data_home, ".index_tmp/", &FileConfig::default())
                .await
                .unwrap();

        Ok(Instance {
            data_store,
            intm,
            state: Arc::new(Mutex::new(HashMap::new())),
            rt: common_runtime::Builder::default().build().unwrap(),
        })
    }

    fn load_options(&self, cli_options: &CliOptions) -> Result<Options> {
        let opts: IndexerOptions = Options::load_layered_options(
            self.config_file.as_deref(),
            self.env_prefix.as_ref(),
            None,
        )?;

        self.convert_options(cli_options, opts)
    }

    pub fn convert_options(
        &self,
        cli_options: &CliOptions,
        mut opts: IndexerOptions,
    ) -> Result<Options> {
        opts.mode = Mode::Standalone;

        if let Some(dir) = &cli_options.log_dir {
            opts.logging.dir = dir.clone();
        }

        if cli_options.log_level.is_some() {
            opts.logging.level = cli_options.log_level.clone();
        }

        let metadata_store = opts.metadata_store.clone();
        let procedure = opts.procedure.clone();
        let frontend = opts.clone().frontend_options();
        let logging = opts.logging.clone();
        let wal_meta = opts.wal.clone().into();
        let datanode = opts.datanode_options().clone();

        Ok(Options::Indexer(Box::new(MixOptions {
            procedure,
            metadata_store,
            data_home: datanode.storage.data_home.to_string(),
            frontend,
            datanode,
            logging,
            wal_meta,
        })))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexerOptions {
    pub mode: Mode,
    pub enable_telemetry: bool,
    pub default_timezone: Option<String>,
    pub http: HttpOptions,
    pub grpc: GrpcOptions,
    pub mysql: MysqlOptions,
    pub postgres: PostgresOptions,
    pub opentsdb: OpentsdbOptions,
    pub influxdb: InfluxdbOptions,
    pub prom_store: PromStoreOptions,
    pub wal: StandaloneWalConfig,
    pub storage: StorageConfig,
    pub metadata_store: KvBackendConfig,
    pub procedure: ProcedureConfig,
    pub logging: LoggingOptions,
    pub user_provider: Option<String>,
    /// Options for different store engines.
    pub region_engine: Vec<RegionEngineConfig>,
    pub export_metrics: ExportMetricsOption,
}

impl Default for IndexerOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Standalone,
            enable_telemetry: true,
            default_timezone: None,
            http: HttpOptions::default(),
            grpc: GrpcOptions::default(),
            mysql: MysqlOptions::default(),
            postgres: PostgresOptions::default(),
            opentsdb: OpentsdbOptions::default(),
            influxdb: InfluxdbOptions::default(),
            prom_store: PromStoreOptions::default(),
            wal: StandaloneWalConfig::default(),
            storage: StorageConfig::default(),
            metadata_store: KvBackendConfig::default(),
            procedure: ProcedureConfig::default(),
            logging: LoggingOptions::default(),
            export_metrics: ExportMetricsOption::default(),
            user_provider: None,
            region_engine: vec![
                RegionEngineConfig::Mito(MitoConfig::default()),
                RegionEngineConfig::File(FileEngineConfig::default()),
            ],
        }
    }
}

impl IndexerOptions {
    fn frontend_options(self) -> FrontendOptions {
        FrontendOptions {
            mode: self.mode,
            default_timezone: self.default_timezone,
            http: self.http,
            grpc: self.grpc,
            mysql: self.mysql,
            postgres: self.postgres,
            opentsdb: self.opentsdb,
            influxdb: self.influxdb,
            prom_store: self.prom_store,
            meta_client: None,
            logging: self.logging,
            user_provider: self.user_provider,
            // Handle the export metrics task run by standalone to frontend for execution
            export_metrics: self.export_metrics,
            ..Default::default()
        }
    }

    fn datanode_options(self) -> DatanodeOptions {
        DatanodeOptions {
            node_id: Some(0),
            enable_telemetry: self.enable_telemetry,
            wal: self.wal.into(),
            storage: self.storage,
            region_engine: self.region_engine,
            rpc_addr: self.grpc.addr,
            ..Default::default()
        }
    }
}
