use super::EvaulateRaError;
use crate::storage;

pub trait MappingInstruction<'expr>: Sized {
    type Input;
    type ConstructContext<'ctx>;

    fn push_nested(input: &'expr Self::Input, pending: &mut Vec<&'expr Self::Input>);

    fn construct<'ctx, SE>(input: &'expr Self::Input, ctx: &Self::ConstructContext<'ctx>) -> Result<Self, EvaulateRaError<SE>>;
    fn evaluate<S>(&self, result_stack: &mut Vec<storage::Data>, row: &storage::Row, engine: &super::NaiveEngine<S>, transaction: &S::TransactionGuard, arena: &bumpalo::Bump) -> impl core::future::Future<Output = Option<storage::Data>> where S: storage::Storage;
}

#[derive(Debug, PartialEq)]
pub struct Mapper<I> {
    pub instruction_stack: Vec<I>,
}

impl<'expr, I> Mapper<I> where I: MappingInstruction<'expr> {
    pub fn construct<'ctx, SE>(start: &'expr I::Input, ctx: I::ConstructContext<'ctx>) -> Result<Self, EvaulateRaError<SE>> {
        let mut pending = Vec::with_capacity(16);
        pending.push(start);

        let mut instructions = Vec::with_capacity(16);
        while let Some(pend) = pending.pop() {
            instructions.push(pend);

            I::push_nested(pend, &mut pending); 
        }

        let mut results: Vec<I> = Vec::with_capacity(instructions.len());

        while let Some(instruction) = instructions.pop() {
            let partial_res = I::construct(instruction, &ctx)?;
            results.push(partial_res); 
        }

        results.reverse();

        Ok(Mapper {
            instruction_stack: results,
        })
    }

    pub async fn evaluate<S>(&self, row: &storage::Row,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump) -> Option<storage::Data> where S: storage::Storage {
        let mut value_stack: Vec<storage::Data> = Vec::with_capacity(self.instruction_stack.len());

        for instr in self.instruction_stack.iter().rev() {
            let value = instr.evaluate(&mut value_stack, row, engine, transaction, arena).await?;
            value_stack.push(value);
        }

        value_stack.pop()
    }
}
