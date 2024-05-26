// Copyright 2024 Greptime Team
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
use std::sync::Arc;

use common_query::error::Result;
use common_query::prelude::{Signature, Volatility};
use common_telemetry::info;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::{BooleanVector, VectorRef};

use crate::function::{Function, FunctionContext};

const NAME: &str = "matches";

/// The function to find remainders
#[derive(Clone, Debug, Default)]
pub struct MatchesFunction;

impl Display for MatchesFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for MatchesFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
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
        let query = columns[1].get(0).as_string().unwrap();
        let tokens = query.split_whitespace().collect::<Vec<_>>();

        info!("query: {:?} {:?}, tokens: {:?}", &columns[1], query, tokens);

        let log = &columns[0];
        let mut matches = vec![];
        for i in 0..log.len() {
            let log = log.get(i).as_string().unwrap();
            let mut found = false;
            for token in &tokens {
                if log.contains(token) {
                    found = true;
                    break;
                }
            }
            matches.push(found);
        }

        // info!("matches function eval: {:?}", columns);
        Ok(Arc::new(BooleanVector::from(matches)))
    }
}
