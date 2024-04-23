use super::EvaulateRaError;
use crate::storage;

pub enum ShortCircuit<V> {
    Nothing(V),
    Skip { amount: usize, result: V },
}

pub trait MappingInstruction<'expr>: Sized {
    type Input;
    type Output;
    type ConstructContext<'ctx>;

    fn push_nested(input: &'expr Self::Input, pending: &mut Vec<&'expr Self::Input>);

    fn construct<'ctx, SE>(
        input: &'expr Self::Input,
        ctx: &Self::ConstructContext<'ctx>,
    ) -> Result<Self, EvaulateRaError<SE>>;

    fn evaluate<S>(
        &self,
        result_stack: &mut Vec<Self::Output>,
        row: &storage::Row,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> impl core::future::Future<Output = Option<Self::Output>>
    where
        S: storage::Storage;

    fn evaluate_mut<S>(
        &mut self,
        result_stack: &mut Vec<Self::Output>,
        row: &storage::Row,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> impl core::future::Future<Output = Option<Self::Output>>
    where
        S: storage::Storage,
    {
        self.evaluate(result_stack, row, engine, transaction, arena)
    }

    fn peek_short_circuit(
        &self,
        outcome: Self::Output,
        _own_index: usize,
        _values: &Vec<Self::Output>,
    ) -> ShortCircuit<Self::Output> {
        ShortCircuit::Nothing(outcome)
    }
}

#[derive(Debug, PartialEq)]
pub struct Mapper<I, V> {
    pub instruction_stack: Vec<I>,
    pub value_stack: Vec<V>,
}

impl<'expr, I> Mapper<I, I::Output>
where
    I: MappingInstruction<'expr>,
{
    pub fn construct<'ctx, SE>(
        start: &'expr I::Input,
        ctx: I::ConstructContext<'ctx>,
    ) -> Result<Self, EvaulateRaError<SE>> {
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

        let value_stack = Vec::with_capacity(results.len());

        Ok(Mapper {
            instruction_stack: results,
            value_stack,
        })
    }

    pub async fn evaluate<S>(
        &self,
        row: &storage::Row,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> Option<I::Output>
    where
        S: storage::Storage,
    {
        let mut value_stack: Vec<I::Output> = Vec::with_capacity(self.instruction_stack.len());

        let mut idx = self.instruction_stack.len() - 1;
        loop {
            let instr = self.instruction_stack.get(idx).expect("We just know");
            let value = instr
                .evaluate(&mut value_stack, row, engine, transaction, arena)
                .await?;

            let (n_idx, value) = match instr.peek_short_circuit(value, idx, &value_stack) {
                ShortCircuit::Nothing(v) => match idx.checked_sub(1) {
                    Some(i) => (i, v),
                    None => return Some(v),
                },
                ShortCircuit::Skip { amount, result } => match idx.checked_sub(amount) {
                    Some(i) => (i, result),
                    None => return Some(result),
                },
            };

            idx = n_idx;
            value_stack.push(value);
        }
    }

    pub async fn evaluate_mut<S>(
        &mut self,
        row: &storage::Row,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> Option<I::Output>
    where
        S: storage::Storage,
    {
        self.value_stack.clear();

        let mut idx = self.instruction_stack.len() - 1;
        loop {
            let instr = self.instruction_stack.get_mut(idx).expect("We just know");
            let value = instr
                .evaluate_mut(&mut self.value_stack, row, engine, transaction, arena)
                .await?;

            let (n_idx, value) = match instr.peek_short_circuit(value, idx, &self.value_stack) {
                ShortCircuit::Nothing(v) => match idx.checked_sub(1) {
                    Some(i) => (i, v),
                    None => return Some(v),
                },
                ShortCircuit::Skip { amount, result } => match idx.checked_sub(amount) {
                    Some(i) => (i, result),
                    None => return Some(result),
                },
            };

            idx = n_idx;
            self.value_stack.push(value);
        }
    }
}
