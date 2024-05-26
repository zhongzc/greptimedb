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

use common_telemetry::info;
use fst::automaton::{Str, Subsequence};
use fst::map::OpBuilder;
use fst::{Automaton, IntoStreamer, Streamer};

use crate::inverted_index::error::{Error, Result};
use crate::inverted_index::search::fst_apply::FstApplier;
use crate::inverted_index::search::predicate::Predicate;
use crate::inverted_index::tokenizer::simple::SimpleTokenizer;
use crate::inverted_index::tokenizer::Tokenizer;
use crate::inverted_index::FstMap;

pub struct FullTextMatchApplier {
    tokens: Vec<String>,
}

impl FstApplier for FullTextMatchApplier {
    fn apply(&self, fst: &FstMap) -> Vec<u64> {
        let mut op = OpBuilder::new();

        for token in &self.tokens {
            op.push(fst.search(Str::new(token)));
        }

        let mut stream = op.union().into_stream();
        let mut values = Vec::new();
        while let Some((_, v)) = stream.next() {
            values.push(v[0].value)
        }
        values
    }

    fn memory_usage(&self) -> usize {
        self.tokens.iter().map(|t| t.len()).sum::<usize>()
            + self.tokens.capacity() * std::mem::size_of::<String>()
    }
}

impl FullTextMatchApplier {
    pub fn try_from(mut predicates: Vec<Predicate>) -> Result<Self> {
        assert!(predicates.len() == 1);
        let predicate = predicates.pop().unwrap();
        match predicate {
            Predicate::FullTextMatch(match_predicate) => {
                let tokens = SimpleTokenizer::default().tokenize(&match_predicate.query);
                info!("tokens: {:?}", tokens);
                Ok(Self { tokens })
            }
            _ => panic!("Expected MatchPredicate"),
        }
    }
}

impl TryFrom<Vec<Predicate>> for FullTextMatchApplier {
    type Error = Error;

    fn try_from(predicates: Vec<Predicate>) -> Result<Self> {
        FullTextMatchApplier::try_from(predicates)
    }
}
