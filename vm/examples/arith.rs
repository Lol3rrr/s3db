use futures::stream::StreamExt;

pub enum MathExpression {
    Value(isize),
    Add {
        left: Box<MathExpression>,
        right: Box<MathExpression>,
    },
}

pub enum Instruction<'i> {
    Value(&'i isize),
    Add,
}

impl<'i> vm::Instruction<'i> for Instruction<'i> {
    type ConstructError = ();
    type Input = MathExpression;
    type Value = isize;
    type Arguments = ();

    async fn construct<'p>(
        input: &'i Self::Input,
        pending: &'p mut Vec<(&'i Self::Input, vm::Output<isize>)>,
        mut out: vm::Output<isize>,
        ctx: &mut vm::ConstructionContext<isize>,
        args: &(),
    ) -> Result<Box<dyn core::future::Future<Output = ()> + 'i>, ()> {
        match input {
            MathExpression::Value(v) => Ok(Box::new(async move {
                let mut done = false;
                loop {
                    if !done {
                        done = true;
                        out.store(*v).await;
                    } else {
                        return;
                    }
                }
            })),
            MathExpression::Add { left, right } => {
                let (mut left_in, left_out) = ctx.io();
                let (mut right_in, right_out) = ctx.io();

                pending.push((&left, left_out));
                pending.push((&right, right_out));

                Ok(Box::new(async move {
                    loop {
                        let left_value: isize = match left_in.get_value().await {
                            Some(v) => v,
                            None => {
                                return;
                            }
                        };
                        dbg!(left_value);

                        let right_value: isize = match right_in.get_value().await {
                            Some(v) => v,
                            None => {
                                return;
                            }
                        };
                        dbg!(right_value);

                        let result = left_value + right_value;
                        dbg!(result);

                        out.store(result).await;
                    }
                }))
            }
        }
    }
}

fn main() {
    let expr = MathExpression::Add {
        left: Box::new(MathExpression::Value(10)),
        right: Box::new(MathExpression::Value(20)),
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut avm = vm::VM::construct::<Instruction>(&expr, &()).await.unwrap();

        let tmp = avm.next().await;
        dbg!(tmp);
    });
}
