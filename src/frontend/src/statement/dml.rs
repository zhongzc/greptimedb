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

use common_error::ext::BoxedError;
use common_query::Output;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use datafusion_expr::{DmlStatement, LogicalPlan as DfLogicalPlan, WriteOp};
use datanode::instance::sql::table_idents_to_full_name;
use datatypes::schema::SchemaRef;
use futures_util::StreamExt;
use query::parser::QueryStatement;
use query::plan::LogicalPlan;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::delete::Delete;
use sql::statements::insert::Insert;
use sql::statements::statement::Statement;
use table::engine::TableReference;
use table::metadata::TableInfoRef;
use table::requests::{DeleteRequest, InsertRequest};
use table::TableRef;

use super::StatementExecutor;
use crate::error::{
    BuildColumnVectorsSnafu, ExecLogicalPlanSnafu, ExecuteStatementSnafu, ExternalSnafu,
    MissingTimeIndexColumnSnafu, ReadRecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::table::insert::handle_insert_request;

impl StatementExecutor {
    pub async fn insert(&self, insert: Box<Insert>, query_ctx: QueryContextRef) -> Result<Output> {
        if insert.can_extract_values() {
            // Fast path: plain insert ("insert with literal values") is executed directly
            let (catalog, schema, table) =
                table_idents_to_full_name(&insert.table_name(), query_ctx.clone())
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;
            let table_ref = TableReference {
                catalog: &catalog,
                schema: &schema,
                table: &table,
            };
            let table = self.get_table(&table_ref).await?;
            let insert_request = build_insert_request_from_stmt_values(&table, &insert)?;
            let affected_rows = handle_insert_request(
                &table.table_info(),
                insert_request,
                query_ctx,
                &self.region_request_handler,
            )
            .await?;

            Ok(Output::AffectedRows(affected_rows))
        } else {
            // Slow path: insert with subquery. Execute the subquery first, via query engine. Then
            // insert the results by sending insert requests.

            // 1. Plan the whole insert statement into a logical plan, then a wrong insert statement
            //    will be caught and a plan error will be returned.
            let statement = QueryStatement::Sql(Statement::Insert(insert));
            let logical_plan = self.plan(statement, query_ctx.clone()).await?;

            // 2. Execute the subquery, get the results as a record batch stream.
            let dml_statement = extract_dml_statement(logical_plan)?;
            ensure!(
                dml_statement.op == WriteOp::Insert,
                UnexpectedSnafu {
                    violated: "expected an INSERT plan"
                }
            );
            let mut stream = self
                .execute_dml_subquery(&dml_statement, query_ctx.clone())
                .await?;

            // 3. Send insert requests.
            let mut affected_rows = 0;
            let table = self.get_table_from_dml(dml_statement, &query_ctx).await?;
            let table_info = table.table_info();
            while let Some(batch) = stream.next().await {
                let record_batch = batch.context(ReadRecordBatchSnafu)?;
                let insert_request = build_insert_request_from_record_batch(
                    record_batch,
                    table.schema(),
                    &table_info,
                )?;
                affected_rows += handle_insert_request(
                    &table_info,
                    insert_request,
                    query_ctx.clone(),
                    &self.region_request_handler,
                )
                .await?;
            }

            Ok(Output::AffectedRows(affected_rows))
        }
    }

    pub async fn delete(&self, delete: Box<Delete>, query_ctx: QueryContextRef) -> Result<Output> {
        // 1. Plan the whole delete statement into a logical plan, then a wrong delete statement
        //    will be caught and a plan error will be returned.
        let statement = QueryStatement::Sql(Statement::Delete(delete));
        let logical_plan = self.plan(statement, query_ctx.clone()).await?;

        // 2. Execute the subquery, get the results as a record batch stream.
        let dml_statement = extract_dml_statement(logical_plan)?;
        ensure!(
            dml_statement.op == WriteOp::Delete,
            UnexpectedSnafu {
                violated: "expected a DELETE plan"
            }
        );
        let mut stream = self
            .execute_dml_subquery(&dml_statement, query_ctx.clone())
            .await?;

        // 3. Send delete requests.
        let mut affected_rows = 0;
        let table = self.get_table_from_dml(dml_statement, &query_ctx).await?;
        let table_info = table.table_info();
        while let Some(batch) = stream.next().await {
            let record_batch = batch.context(ReadRecordBatchSnafu)?;
            let delete_request =
                build_delete_request_from_record_batch(record_batch, table.schema(), &table_info)?;
            affected_rows += self
                .send_delete_request(delete_request, query_ctx.clone())
                .await?;
        }

        Ok(Output::AffectedRows(affected_rows))
    }

    async fn execute_dml_subquery(
        &self,
        dml_statement: &DmlStatement,
        query_ctx: QueryContextRef,
    ) -> Result<SendableRecordBatchStream> {
        let subquery_plan = LogicalPlan::from(dml_statement.input.as_ref().clone());
        let output = self
            .query_engine
            .execute(subquery_plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)?;
        match output {
            Output::Stream(stream) => Ok(stream),
            Output::RecordBatches(record_batches) => Ok(record_batches.as_stream()),
            _ => UnexpectedSnafu {
                violated: "expected a stream",
            }
            .fail(),
        }
    }

    async fn get_table_from_dml(
        &self,
        dml_statement: DmlStatement,
        query_ctx: &QueryContextRef,
    ) -> Result<TableRef> {
        let default_catalog = query_ctx.current_catalog().to_owned();
        let default_schema = query_ctx.current_schema().to_owned();
        let resolved_table_ref = dml_statement
            .table_name
            .resolve(&default_catalog, &default_schema);
        let table_ref = TableReference::full(
            &resolved_table_ref.catalog,
            &resolved_table_ref.schema,
            &resolved_table_ref.table,
        );
        self.get_table(&table_ref).await
    }
}

fn extract_dml_statement(logical_plan: LogicalPlan) -> Result<DmlStatement> {
    let LogicalPlan::DfPlan(df_plan) = logical_plan;
    match df_plan {
        DfLogicalPlan::Dml(dml) => Ok(dml),
        _ => UnexpectedSnafu {
            violated: "expected a DML plan",
        }
        .fail(),
    }
}

fn build_insert_request_from_record_batch(
    record_batch: RecordBatch,
    table_schema: SchemaRef,
    table_info: &TableInfoRef,
) -> Result<InsertRequest> {
    let columns_values = record_batch
        .column_vectors(&table_info.name, table_schema)
        .context(BuildColumnVectorsSnafu)?;

    Ok(InsertRequest {
        catalog_name: table_info.catalog_name.clone(),
        schema_name: table_info.schema_name.clone(),
        table_name: table_info.name.clone(),
        columns_values,
        region_number: 0,
    })
}

pub fn build_insert_request_from_stmt_values(
    table: &TableRef,
    stmt: &Insert,
) -> Result<InsertRequest> {
    todo!();

    // let values = stmt.values_body().context(MissingInsertBodySnafu)?;

    // let columns = stmt.columns();
    // let schema = table.schema();
    // let columns_num = if columns.is_empty() {
    //     schema.column_schemas().len()
    // } else {
    //     columns.len()
    // };
    // let rows_num = values.len();

    // let mut columns_builders: Vec<(&ColumnSchema, Box<dyn MutableVector>)> =
    //     Vec::with_capacity(columns_num);

    // // Initialize vectors
    // if columns.is_empty() {
    //     for column_schema in schema.column_schemas() {
    //         let data_type = &column_schema.data_type;
    //         columns_builders.push((column_schema, data_type.create_mutable_vector(rows_num)));
    //     }
    // } else {
    //     for column_name in columns {
    //         let column_schema =
    //             schema
    //                 .column_schema_by_name(column_name)
    //                 .with_context(|| ColumnNotFoundSnafu {
    //                     msg: format!(
    //                         "Column {} not found in table {}",
    //                         column_name,
    //                         table.table_info().name
    //                     ),
    //                 })?;
    //         let data_type = &column_schema.data_type;
    //         columns_builders.push((column_schema, data_type.create_mutable_vector(rows_num)));
    //     }
    // }

    // // Convert rows into columns
    // for row in values {
    //     ensure!(
    //         row.len() == columns_num,
    //         ColumnValuesNumberMismatchSnafu {
    //             columns: columns_num,
    //             values: row.len(),
    //         }
    //     );

    //     for (sql_val, (column_schema, builder)) in row.iter().zip(columns_builders.iter_mut()) {
    //         add_row_to_vector(column_schema, sql_val, builder)?;
    //     }
    // }

    // Ok(InsertRequest {
    //     catalog_name: table_ref.catalog.to_string(),
    //     schema_name: table_ref.schema.to_string(),
    //     table_name: table_ref.table.to_string(),
    //     columns_values: columns_builders
    //         .into_iter()
    //         .map(|(cs, mut b)| (cs.name.to_string(), b.to_vector()))
    //         .collect(),
    //     region_number: 0,
    // })
}

const DEFAULT_PLACEHOLDER_VALUE: &str = "default";

// fn add_row_to_vector(
//     column_schema: &ColumnSchema,
//     sql_val: &SqlValue,
//     builder: &mut Box<dyn MutableVector>,
// ) -> Result<()> {
//     let value = if replace_default(sql_val) {
//         column_schema
//             .create_default()
//             .context(ColumnDefaultValueSnafu {
//                 column: column_schema.name.to_string(),
//             })?
//             .context(ColumnNoneDefaultValueSnafu {
//                 column: column_schema.name.to_string(),
//             })?
//     } else {
//         statements::sql_value_to_value(&column_schema.name, &column_schema.data_type, sql_val)
//             .context(ParseSqlSnafu)?
//     };
//     builder.push_value_ref(value.as_value_ref());
//     Ok(())
// }

// fn replace_default(sql_val: &SqlValue) -> bool {
//     matches!(sql_val, SqlValue::Placeholder(s) if s.to_lowercase() == DEFAULT_PLACEHOLDER_VALUE)
// }

fn build_delete_request_from_record_batch(
    record_batch: RecordBatch,
    table_schema: SchemaRef,
    table_info: &TableInfoRef,
) -> Result<DeleteRequest> {
    let ts_column = table_schema
        .timestamp_column()
        .map(|x| x.name.clone())
        .with_context(|| table::error::MissingTimeIndexColumnSnafu {
            table_name: table_info.name.clone(),
        })
        .context(MissingTimeIndexColumnSnafu)?;

    let column_vectors = record_batch
        .column_vectors(&table_info.name, table_schema)
        .context(BuildColumnVectorsSnafu)?;

    let rowkey_columns = table_info
        .meta
        .row_key_column_names()
        .collect::<Vec<&String>>();

    let key_column_values = column_vectors
        .into_iter()
        .filter(|x| x.0 == ts_column || rowkey_columns.contains(&&x.0))
        .collect::<HashMap<_, _>>();

    Ok(DeleteRequest {
        catalog_name: table_info.catalog_name.clone(),
        schema_name: table_info.schema_name.clone(),
        table_name: table_info.name.clone(),
        key_column_values,
    })
}
