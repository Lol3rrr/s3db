use super::EvaulateRaError;
use std::borrow::Cow;


pub enum ShortCircuit<'vs, V> where V: Clone + ToOwned {
    Nothing(Cow<'vs, V>),
    Skip { amount: usize, result: Cow<'vs, V> },
}

pub trait MappingInstruction<'expr>: Sized {
    type Input;
    type Output: Clone;
    type ConstructContext<'ctx>;

    fn push_nested(input: &'expr Self::Input, pending: &mut Vec<&'expr Self::Input>);

    fn construct<'ctx, SE>(
        input: &'expr Self::Input,
        ctx: &Self::ConstructContext<'ctx>,
    ) -> Result<Self, EvaulateRaError<SE>>;

    fn evaluate<'vs, 'row, S>(
        &self,
        result_stack: &mut Vec<Cow<'vs, Self::Output>>,
        row: &'row storage::RowCow<'_>,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> impl core::future::Future<Output = Option<Cow<'vs, Self::Output>>>
    where
        S: storage::Storage, 'row: 'vs;

    fn evaluate_mut<'vs, 'row, S>(
        &mut self,
        result_stack: &mut Vec<Cow<'vs, Self::Output>>,
        row: &'row storage::RowCow<'_>,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> impl core::future::Future<Output = Option<Cow<'vs, Self::Output>>>
    where
        S: storage::Storage, 'row: 'vs,
    {
        self.evaluate(result_stack, row, engine, transaction, arena)
    }

    fn peek_short_circuit<'vs>(
        &self,
        outcome: Cow<'vs, Self::Output>,
        _own_index: usize,
        _values: &Vec<Cow<'_, Self::Output>>,
    ) -> ShortCircuit<'vs, Self::Output> {
        ShortCircuit::Nothing(outcome)
    }
}

#[derive(Debug)]
pub struct Mapper<I, V> where V: 'static + Clone {
    pub instruction_stack: Vec<I>,
    pub value_stack: Vec<std::borrow::Cow<'static, V>>,
}

impl<I, V> PartialEq for Mapper<I, V> where I: PartialEq, V: 'static + PartialEq + ToOwned + Clone {
    fn eq(&self, other: &Self) -> bool {
        if self.instruction_stack != other.instruction_stack {
            return false;
        }

        self.value_stack.iter().zip(other.value_stack.iter()).all(|(f, s)| f.as_ref() == s.as_ref())
    }
}

impl<'expr, I> Mapper<I, I::Output>
where
    I: MappingInstruction<'expr>,
{
    fn shorten_stack_lifetime<'r, 'og, 'target>(input: &'r mut Vec<Cow<'og, I::Output>>) -> &'r mut Vec<Cow<'target, I::Output>> where 'og: 'target {
        assert!(input.is_empty());
        unsafe { core::mem::transmute(input) }
    }

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
        row: &storage::RowCow<'_>,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> Option<I::Output>
    where
        S: storage::Storage,
    {
        let mut value_stack: Vec<Cow<'_, I::Output>> = Vec::with_capacity(self.instruction_stack.len());

        let mut idx = self.instruction_stack.len() - 1;
        loop {
            let instr = self.instruction_stack.get(idx).expect("We just know");
            let value = instr
                .evaluate(&mut value_stack, row, engine, transaction, arena)
                .await?;

            let (n_idx, value) = match instr.peek_short_circuit(value, idx, &value_stack) {
                ShortCircuit::Nothing(v) => match idx.checked_sub(1) {
                    Some(i) => (i, v),
                    None => return Some(v.into_owned()),
                },
                ShortCircuit::Skip { amount, result } => match idx.checked_sub(amount) {
                    Some(i) => (i, result),
                    None => return Some(result.into_owned()),
                },
            };

            idx = n_idx;
            value_stack.push(value);
        }
    }

    pub async fn evaluate_mut<S>(
        &mut self,
        row: &storage::RowCow<'_>,
        engine: &super::NaiveEngine<S>,
        transaction: &S::TransactionGuard,
        arena: &bumpalo::Bump,
    ) -> Option<I::Output>
    where
        S: storage::Storage,
    {
        self.value_stack.clear();
        let stack = Self::shorten_stack_lifetime(&mut self.value_stack);

        let mut idx = self.instruction_stack.len() - 1;
        loop {
            let instr = self.instruction_stack.get_mut(idx).expect("We just know");
            let value = instr
                .evaluate_mut(stack, row, engine, transaction, arena)
                .await?;

            let (n_idx, value) = match instr.peek_short_circuit(value, idx, &stack) {
                ShortCircuit::Nothing(v) => match idx.checked_sub(1) {
                    Some(i) => (i, v),
                    None => return Some(v.into_owned()),
                },
                ShortCircuit::Skip { amount, result } => match idx.checked_sub(amount) {
                    Some(i) => (i, result),
                    None => return Some(result.into_owned()),
                },
            };

            idx = n_idx;
            stack.push(value);
        }
    }
}
