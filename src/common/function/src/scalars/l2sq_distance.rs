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

use std::fmt;
use std::fmt::Display;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::vectors::VectorRef;
use snafu::ensure;

use crate::function::{Function, FunctionContext};
const NAME: &str = "l2sq_distance";

#[derive(Clone, Debug, Default)]
pub struct L2SqrDistranceFunction;

impl Display for L2SqrDistranceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for L2SqrDistranceFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::float32_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::string_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect 2, have: {}",
                    columns.len()
                ),
            }
        );

        let left = &columns[0];
        let right = &columns[1];

        let size = left.len();
        let mut result = ConcreteDataType::float32_datatype().create_mutable_vector(size);
        for i in 0..size {
            let left_value = left.get(i).as_string().unwrap();
            let right_value = right.get(i).as_string().unwrap();

            // Vectors like "[1.0, 2.0, 3.0]", need to parse them to get the values
            let left_value = left_value
                .trim_matches(|c| c == '[' || c == ']')
                .split(',')
                .map(|s| s.trim().parse::<f32>().unwrap())
                .collect::<Vec<f32>>();
            let right_value = right_value
                .trim_matches(|c| c == '[' || c == ']')
                .split(',')
                .map(|s| s.trim().parse::<f32>().unwrap())
                .collect::<Vec<f32>>();

            let d: f32 = <f32 as simsimd::SpatialSimilarity>::l2sq(&left_value, &right_value)
                .unwrap() as f32;
            result.push_value_ref(d.into());
        }

        Ok(result.to_vector())
    }
}

//
