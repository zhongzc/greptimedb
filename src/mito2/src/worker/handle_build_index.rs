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

use std::sync::Arc;

use common_query::Output;
use common_telemetry::{debug, error, info, warn};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{SerializedFileReader, SerializedRowGroupReader};
use snafu::ResultExt;
use store_api::metadata::{RegionMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::region_request::RegionAlterRequest;
use store_api::storage::RegionId;

use crate::error::{InvalidMetadataSnafu, InvalidRegionRequestSnafu, Result};
use crate::flush::FlushReason;
use crate::manifest::action::{RegionChange, RegionMetaAction, RegionMetaActionList};
use crate::memtable::MemtableBuilderRef;
use crate::region::version::Version;
use crate::region::MitoRegionRef;
use crate::request::{DdlRequest, OptionOutputTx, SenderDdlRequest};
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

        sender.send(IndexBuilder::new(region).build().await)
    }
}

pub struct IndexBuilder {
    region: MitoRegionRef,
}

impl IndexBuilder {
    fn new(region: MitoRegionRef) -> Self {
        Self { region }
    }
    async fn build(&self) -> Result<Output> {
        let current_version = self.region.version_control.current();
        for level in current_version.version.ssts.levels() {
            for (file_id, file_handle) in &level.files {
                let parquet_file_reader = self
                    .region
                    .access_layer
                    .sst_file_reader(file_handle.clone());
                self.build_index(file_id, parquet_file_reader).await;
            }
        }

        Ok(Output::AffectedRows(0))
    }

    async fn build_index(&self, file_id: &FileId, parquet_file_reader: Arc<dyn FileReader>) {
        info!("Building index for file {}", file_id);

        for i in 0..parquet_file_reader.num_row_groups() {
            let row_group_reader = parquet_file_reader
                .get_row_group(i)
                .expect("Failed to get row group reader");

            let num_columns = row_group_reader.num_columns();
            let num_rows = row_group_reader.metadata().num_rows();
            info!(
                "Row group info: row group {}, num_rows {}, num_columns {}",
                i, num_rows, num_columns
            );
        }
    }
}
