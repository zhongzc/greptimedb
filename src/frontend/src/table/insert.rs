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

use std::collections::HashMap;

use api::helper::{push_vals, vectors_to_rows, ColumnDataTypeWrapper};
use api::v1::column::Values;
use api::v1::region::{InsertRequest as GrpcInsertRequest, InsertRequests, region_request};
use api::v1::{Column, ColumnSchema, Rows, SemanticType};
use datatypes::prelude::*;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::metadata::{TableInfoRef, TableMeta};
use table::requests::InsertRequest;

use crate::error::{
    self, ColumnDataTypeSnafu, ColumnNotFoundSnafu, InvalidInsertRequestSnafu,
    MissingTimeIndexColumnSnafu, NotSupportedSnafu, Result, VectorToGrpcColumnSnafu,
};
use crate::instance::region_handler::RegionRequestHandlerRef;

pub(crate) async fn handle_insert_request(
    table_info: &TableInfoRef,
    request: InsertRequest,
    ctx: QueryContextRef,
    region_request_handler: &RegionRequestHandlerRef,
) -> Result<usize> {
    let insert_request = to_grpc_insert_request(table_info, request)?;
    let insert_request = region_request::Body::Inserts(InsertRequests {
        requests: vec![insert_request],
    });
    let affected_rows = region_request_handler
        .handle(insert_request, ctx)
        .await?
        .affected_rows;
    Ok(affected_rows as _)
}

pub(crate) fn to_grpc_columns(
    table_meta: &TableMeta,
    columns_values: &HashMap<String, VectorRef>,
) -> Result<(Vec<Column>, u32)> {
    let mut row_count = None;

    let columns = columns_values
        .iter()
        .map(|(column_name, vector)| {
            match row_count {
                Some(rows) => ensure!(
                    rows == vector.len(),
                    error::InvalidInsertRequestSnafu {
                        reason: "The row count of columns is not the same."
                    }
                ),

                None => row_count = Some(vector.len()),
            }

            let column = vector_to_grpc_column(table_meta, column_name, vector.clone())?;
            Ok(column)
        })
        .collect::<Result<Vec<_>>>()?;

    let row_count = row_count.unwrap_or(0) as u32;

    Ok((columns, row_count))
}

pub(crate) fn to_grpc_insert_request(
    table_info: &TableInfoRef,
    insert: InsertRequest,
) -> Result<GrpcInsertRequest> {
    let region_id = RegionId::new(table_info.table_id(), insert.region_number).into();
    let row_count = row_count(&insert.columns_values)?;
    let schema = column_schema(table_info, &insert.columns_values)?;
    let rows = vectors_to_rows(insert.columns_values.values(), row_count);
    Ok(GrpcInsertRequest {
        region_id,
        rows: Some(Rows { schema, rows }),
    })
}

pub(crate) fn row_count(columns: &HashMap<String, VectorRef>) -> Result<usize> {
    let mut columns_iter = columns.values();

    let len = columns_iter
        .next()
        .map(|column| column.len())
        .unwrap_or_default();
    ensure!(
        columns_iter.all(|column| column.len() == len),
        InvalidInsertRequestSnafu {
            reason: "The row count of columns is not the same."
        }
    );

    Ok(len)
}

pub(crate) fn column_schema(
    table_info: &TableInfoRef,
    columns: &HashMap<String, VectorRef>,
) -> Result<Vec<ColumnSchema>> {
    let table_meta = &table_info.meta;
    let mut schema = vec![];

    for (column_name, vector) in columns {
        let time_index_column = &table_meta
            .schema
            .timestamp_column()
            .with_context(|| table::error::MissingTimeIndexColumnSnafu {
                table_name: table_info.name.to_string(),
            })
            .context(MissingTimeIndexColumnSnafu)?
            .name;
        let semantic_type = if column_name == time_index_column {
            SemanticType::Timestamp
        } else {
            let column_index = table_meta
                .schema
                .column_index_by_name(column_name)
                .context(ColumnNotFoundSnafu {
                    msg: format!("unable to find column {column_name} in table schema"),
                })?;
            if table_meta.primary_key_indices.contains(&column_index) {
                SemanticType::Tag
            } else {
                SemanticType::Field
            }
        };

        let datatype: ColumnDataTypeWrapper =
            vector.data_type().try_into().context(ColumnDataTypeSnafu)?;

        schema.push(ColumnSchema {
            column_name: column_name.clone(),
            datatype: datatype.datatype().into(),
            semantic_type: semantic_type.into(),
        });
    }

    Ok(schema)
}

fn vector_to_grpc_column(
    table_meta: &TableMeta,
    column_name: &str,
    vector: VectorRef,
) -> Result<Column> {
    let time_index_column = &table_meta
        .schema
        .timestamp_column()
        .context(NotSupportedSnafu {
            feat: "Table without time index.",
        })?
        .name;
    let semantic_type = if column_name == time_index_column {
        SemanticType::Timestamp
    } else {
        let column_index = table_meta
            .schema
            .column_index_by_name(column_name)
            .context(VectorToGrpcColumnSnafu {
                reason: format!("unable to find column {column_name} in table schema"),
            })?;
        if table_meta.primary_key_indices.contains(&column_index) {
            SemanticType::Tag
        } else {
            SemanticType::Field
        }
    };

    let datatype: ColumnDataTypeWrapper =
        vector.data_type().try_into().context(ColumnDataTypeSnafu)?;

    let mut column = Column {
        column_name: column_name.to_string(),
        semantic_type: semantic_type as i32,
        null_mask: vec![],
        datatype: datatype.datatype() as i32,
        values: Some(Values::default()), // vector values will be pushed into it below
    };
    push_vals(&mut column, 0, vector);
    Ok(column)
}

fn build_insert_request(
    table: &TableRef,
    stmt: &Insert,
) -> Result<InsertRequest> {
    let values = stmt.values_body().context(MissingInsertBodySnafu)?;

    let columns = stmt.columns();
    let schema = table.schema();
    let columns_num = if columns.is_empty() {
        schema.column_schemas().len()
    } else {
        columns.len()
    };
    let rows_num = values.len();

    let mut columns_builders: Vec<(&ColumnSchema, Box<dyn MutableVector>)> =
        Vec::with_capacity(columns_num);

    // Initialize vectors
    if columns.is_empty() {
        for column_schema in schema.column_schemas() {
            let data_type = &column_schema.data_type;
            columns_builders.push((column_schema, data_type.create_mutable_vector(rows_num)));
        }
    } else {
        for column_name in columns {
            let column_schema =
                schema.column_schema_by_name(column_name).with_context(|| {
                    ColumnNotFoundSnafu {
                        msg: format!(
                            "Column {} not found in table {}",
                            column_name, table.table_info().name
                        )
                    }
                })?;
            let data_type = &column_schema.data_type;
            columns_builders.push((column_schema, data_type.create_mutable_vector(rows_num)));
        }
    }

    // Convert rows into columns
    for row in values {
        ensure!(
            row.len() == columns_num,
            ColumnValuesNumberMismatchSnafu {
                columns: columns_num,
                values: row.len(),
            }
        );

        for (sql_val, (column_schema, builder)) in row.iter().zip(columns_builders.iter_mut()) {
            add_row_to_vector(column_schema, sql_val, builder)?;
        }
    }

    Ok(InsertRequest {
        catalog_name: table_ref.catalog.to_string(),
        schema_name: table_ref.schema.to_string(),
        table_name: table_ref.table.to_string(),
        columns_values: columns_builders
            .into_iter()
            .map(|(cs, mut b)| (cs.name.to_string(), b.to_vector()))
            .collect(),
        region_number: 0,
    })
}


const DEFAULT_PLACEHOLDER_VALUE: &str = "default";

fn add_row_to_vector(
    column_schema: &ColumnSchema,
    sql_val: &SqlValue,
    builder: &mut Box<dyn MutableVector>,
) -> Result<()> {
    let value = if replace_default(sql_val) {
        column_schema
            .create_default()
            .context(ColumnDefaultValueSnafu {
                column: column_schema.name.to_string(),
            })?
            .context(ColumnNoneDefaultValueSnafu {
                column: column_schema.name.to_string(),
            })?
    } else {
        statements::sql_value_to_value(&column_schema.name, &column_schema.data_type, sql_val)
            .context(ParseSqlSnafu)?
    };
    builder.push_value_ref(value.as_value_ref());
    Ok(())
}

fn replace_default(sql_val: &SqlValue) -> bool {
    matches!(sql_val, SqlValue::Placeholder(s) if s.to_lowercase() == DEFAULT_PLACEHOLDER_VALUE)
}


// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;
//     use std::sync::Arc;

//     use api::v1::ColumnDataType;
//     use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
//     use datatypes::prelude::ScalarVectorBuilder;
//     use datatypes::schema::{ColumnSchema, Schema};
//     use datatypes::vectors::{
//         Int16VectorBuilder, Int32Vector, Int64Vector, MutableVector, StringVector,
//         StringVectorBuilder,
//     };
//     use table::metadata::TableMetaBuilder;
//     use table::requests::InsertRequest;

//     use super::*;

//     #[test]
//     fn test_vector_to_grpc_column() {
//         let schema = Arc::new(Schema::new(vec![
//             ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false)
//                 .with_time_index(true),
//             ColumnSchema::new("k", ConcreteDataType::int32_datatype(), false),
//             ColumnSchema::new("v", ConcreteDataType::string_datatype(), true),
//         ]));

//         let table_meta = TableMetaBuilder::default()
//             .schema(schema)
//             .primary_key_indices(vec![1])
//             .next_column_id(3)
//             .build()
//             .unwrap();

//         let column = vector_to_grpc_column(
//             &table_meta,
//             "ts",
//             Arc::new(Int64Vector::from_slice([1, 2, 3])),
//         )
//         .unwrap();
//         assert_eq!(column.column_name, "ts");
//         assert_eq!(column.semantic_type, SemanticType::Timestamp as i32);
//         assert_eq!(column.values.unwrap().i64_values, vec![1, 2, 3]);
//         assert_eq!(column.null_mask, vec![0]);
//         assert_eq!(column.datatype, ColumnDataType::Int64 as i32);

//         let column = vector_to_grpc_column(
//             &table_meta,
//             "k",
//             Arc::new(Int32Vector::from_slice([3, 2, 1])),
//         )
//         .unwrap();
//         assert_eq!(column.column_name, "k");
//         assert_eq!(column.semantic_type, SemanticType::Tag as i32);
//         assert_eq!(column.values.unwrap().i32_values, vec![3, 2, 1]);
//         assert_eq!(column.null_mask, vec![0]);
//         assert_eq!(column.datatype, ColumnDataType::Int32 as i32);

//         let column = vector_to_grpc_column(
//             &table_meta,
//             "v",
//             Arc::new(StringVector::from(vec![
//                 Some("hello"),
//                 None,
//                 Some("greptime"),
//             ])),
//         )
//         .unwrap();
//         assert_eq!(column.column_name, "v");
//         assert_eq!(column.semantic_type, SemanticType::Field as i32);
//         assert_eq!(
//             column.values.unwrap().string_values,
//             vec!["hello", "greptime"]
//         );
//         assert_eq!(column.null_mask, vec![2]);
//         assert_eq!(column.datatype, ColumnDataType::String as i32);
//     }

//     #[test]
//     fn test_to_grpc_insert_request() {
//         let schema = Schema::new(vec![
//             ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), false)
//                 .with_time_index(true),
//             ColumnSchema::new("id", ConcreteDataType::int16_datatype(), false),
//             ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
//         ]);

//         let table_meta = TableMetaBuilder::default()
//             .schema(Arc::new(schema))
//             .primary_key_indices(vec![])
//             .next_column_id(3)
//             .build()
//             .unwrap();

//         let insert_request = mock_insert_request();
//         let request = to_grpc_insert_request(&table_meta, 12, insert_request).unwrap();

//         verify_grpc_insert_request(request);
//     }

//     fn mock_insert_request() -> InsertRequest {
//         let mut builder = StringVectorBuilder::with_capacity(3);
//         builder.push(Some("host1"));
//         builder.push(None);
//         builder.push(Some("host3"));
//         let host = builder.to_vector();

//         let mut builder = Int16VectorBuilder::with_capacity(3);
//         builder.push(Some(1_i16));
//         builder.push(Some(2_i16));
//         builder.push(Some(3_i16));
//         let id = builder.to_vector();

//         let columns_values = HashMap::from([("host".to_string(), host), ("id".to_string(), id)]);

//         InsertRequest {
//             catalog_name: DEFAULT_CATALOG_NAME.to_string(),
//             schema_name: DEFAULT_SCHEMA_NAME.to_string(),
//             table_name: "demo".to_string(),
//             columns_values,
//             region_number: 0,
//         }
//     }

//     fn verify_grpc_insert_request(request: GrpcInsertRequest) {
//         let table_name = request.table_name;
//         assert_eq!("demo", table_name);

//         for column in request.columns {
//             let name = column.column_name;
//             if name == "id" {
//                 assert_eq!(0, column.null_mask[0]);
//                 assert_eq!(ColumnDataType::Int16 as i32, column.datatype);
//                 assert_eq!(vec![1, 2, 3], column.values.as_ref().unwrap().i16_values);
//             }
//             if name == "host" {
//                 assert_eq!(2, column.null_mask[0]);
//                 assert_eq!(ColumnDataType::String as i32, column.datatype);
//                 assert_eq!(
//                     vec!["host1", "host3"],
//                     column.values.as_ref().unwrap().string_values
//                 );
//             }
//         }

//         let region_number = request.region_number;
//         assert_eq!(12, region_number);
//     }
// }
