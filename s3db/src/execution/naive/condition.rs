use super::{NaiveEngine, EvaulateRaError};

use std::collections::HashMap;

use futures::{future::LocalBoxFuture, FutureExt};

use crate::{
    ra::{self, AttributeId}, storage
};
use sql::DataType;

#[derive(Debug)]
enum ConditionInstruction {
    And {
        count: usize,
        source: usize,
        idx: usize,
    },
    Or {
        count: usize,
        source: usize,
        idx: usize,
    },
    Value {
        source: usize,
        idx: usize,
    },
}

pub async fn evaluate_ra_cond<'s, 'cond, 'r, 'col, 'p, 'c, 'o, 't, 'f, 'arena, S>(
    engine: &'s NaiveEngine<S>,
    condition: &'cond ra::RaCondition,
    row: &'r storage::Row,
    columns: &'col [(String, DataType, AttributeId)],
    placeholders: &'p HashMap<usize, storage::Data>,
    ctes: &'c HashMap<String, storage::EntireRelation>,
    outer: &'o HashMap<AttributeId, storage::Data>,
    transaction: &'t S::TransactionGuard,
    arena: &'arena bumpalo::Bump
) -> Result<bool, EvaulateRaError<S::LoadingError>>
where
    's: 'f,
    'cond: 'f,
    'r: 'f,
    'col: 'f,
    'p: 'f,
    'c: 'f,
    'o: 'f,
    't: 'f,
    'arena: 'f,
    S: crate::storage::Storage,
{
        let mut pending = vec![(0, 0, condition)];
        let mut instructions = Vec::new();
        let mut expressions = Vec::new();
        while let Some((src, idx, tmp)) = pending.pop() {
            match tmp {
                ra::RaCondition::Or(parts) => {
                    instructions.push(ConditionInstruction::Or {
                        count: parts.len(),
                        source: src,
                        idx,
                    });
                    pending.extend(parts.iter().enumerate().rev().map(|(idx, p)| (instructions.len(), idx, p)));
                }
                ra::RaCondition::And(parts) => {
                    instructions.push(ConditionInstruction::And {
                        count: parts.len(),
                        source: src,
                        idx,
                    });
                    pending.extend(parts.iter().enumerate().rev().map(|(idx, p)| (instructions.len(), idx, p)));
                }
                ra::RaCondition::Value(val) => {
                    expressions.push(val);
                    instructions.push(ConditionInstruction::Value {
                        source: src,
                        idx,
                    });
                }
            };
        }


        let mut results = Vec::new();
        while let Some(instruction) = instructions.pop() {
            let (src, instr_idx, outcome) = match instruction {
                ConditionInstruction::Value { source, idx } => {
                    let value = expressions.pop().unwrap();
                    let res = engine.evaluate_ra_cond_val(value, row, columns, placeholders, ctes, outer,transaction, &arena).await?;

                    (source, idx, res)
                }
                ConditionInstruction::And { count, source, idx } => {
                    let mut outcome = true;    
                    for _ in 0..count {
                        outcome = outcome && results.pop().unwrap();
                    }

                    (source, idx, outcome)
                }
                ConditionInstruction::Or { count, source, idx } => {
                    let mut outcome = false;
                    for _ in 0..count {
                        outcome = outcome || results.pop().unwrap();
                    }

                    (source, idx, outcome)
                }
            };

            let src_index = match src.checked_sub(1) {
                Some(idx) => idx,
                None => {
                    return Ok(outcome);
                }
            };
            match instructions.get(src_index).unwrap() {
                ConditionInstruction::And { count, .. } => {
                    if outcome {
                        results.push(true);
                    } else {
                        let count = *count;

                        let removed_instructions = instructions.drain(src_index+1..);
                        for instr in removed_instructions {
                            match instr {
                                ConditionInstruction::Value {  .. } => {
                                    expressions.pop();
                                }
                                _ => {}
                            };
                        }
                        
                        results.push(false);
                        results.extend(core::iter::repeat(false).take(count - instr_idx - 1));
                    }
                }
                ConditionInstruction::Or { count, .. } => {
                    if !outcome {
                        results.push(false);
                    } else {
                        let count = *count;

                        let removed_instructions = instructions.drain(src_index+1..);
                        for instr in removed_instructions {
                            match instr {
                                ConditionInstruction::Value {  .. } => {
                                    expressions.pop();
                                }
                                _ => {}
                            };
                        }
                    
                        results.push(true);
                        results.extend(core::iter::repeat(true).take(count - instr_idx - 1));
                    }
                }
                ConditionInstruction::Value { .. } => unreachable!(),
            };
        }

        Ok(false)
}
