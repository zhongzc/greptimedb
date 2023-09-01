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

use catalog::CatalogManagerRef;
use common_catalog::format_full_table_name;
use datatypes::data_type::DataType;
use datatypes::schema::ColumnSchema;
use datatypes::vectors::MutableVector;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::Value as SqlValue;
use sql::statements;
use sql::statements::insert::Insert;
use table::engine::TableReference;
use table::requests::*;
use table::TableRef;

use crate::error::{
    CatalogSnafu, ColumnDefaultValueSnafu, ColumnNoneDefaultValueSnafu, ColumnNotFoundSnafu,
    ColumnValuesNumberMismatchSnafu, MissingInsertBodySnafu, ParseSqlSnafu, Result,
    TableNotFoundSnafu,
};
use crate::sql::{table_idents_to_full_name, SqlHandler};
