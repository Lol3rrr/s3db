use futures::stream::StreamExt;

pub enum Expression {
    Constant(isize),
    RepeatingValue(isize),
    Add { left: Box<Self>, right: Box<Self> },
    Sub { left: Box<Self>, right: Box<Self> },
    Mult { left: Box<Self>, right: Box<Self> },
    Div { left: Box<Self>, right: Box<Self> },
}

pub struct MathInstruction {}

impl<'i> vm::Instruction<'i> for MathInstruction {
    type ConstructError = ();
    type Input = Expression;
    type Value = isize;
    type Arguments<'args> = () where 'args: 'i, Self: 'args;

    async fn construct<'p, 'args>(
        input: &'i Self::Input,
        pending: &'p mut Vec<(&'i Self::Input, vm::Output<Self::Value>)>,
        mut out: vm::Output<Self::Value>,
        ctx: &mut vm::ConstructionContext<Self::Value>,
        args: &Self::Arguments<'args>,
    ) -> Result<Box<dyn core::future::Future<Output = ()> + 'i>, ()>
    where
        'args: 'i,
    {
        match input {
            Expression::Constant(v) => Ok(Box::new(async move {
                out.store(*v).await;
            })),
            Expression::RepeatingValue(v) => Ok(Box::new(async move {
                loop {
                    out.store(*v).await;
                }
            })),
            Expression::Add { left, right } => {
                let (mut left_in, left_out) = ctx.io();
                let (mut right_in, right_out) = ctx.io();

                pending.push((left, left_out));
                pending.push((right, right_out));

                Ok(Box::new(async move {
                    loop {
                        let lvalue = match left_in.get_value().await {
                            Some(v) => v,
                            None => return,
                        };
                        let rvalue = match right_in.get_value().await {
                            Some(v) => v,
                            None => return,
                        };

                        let result = lvalue + rvalue;

                        out.store(result).await;
                    }
                }))
            }
            Expression::Sub { left, right } => {
                let (mut left_in, left_out) = ctx.io();
                let (mut right_in, right_out) = ctx.io();

                pending.push((left, left_out));
                pending.push((right, right_out));

                Ok(Box::new(async move {
                    loop {
                        let lvalue = match left_in.get_value().await {
                            Some(v) => v,
                            None => return,
                        };
                        let rvalue = match right_in.get_value().await {
                            Some(v) => v,
                            None => return,
                        };

                        let result = lvalue - rvalue;

                        out.store(result).await;
                    }
                }))
            }
            Expression::Mult { left, right } => {
                let (mut left_in, left_out) = ctx.io();
                let (mut right_in, right_out) = ctx.io();

                pending.push((left, left_out));
                pending.push((right, right_out));

                Ok(Box::new(async move {
                    loop {
                        let lvalue = match left_in.get_value().await {
                            Some(v) => v,
                            None => return,
                        };
                        let rvalue = match right_in.get_value().await {
                            Some(v) => v,
                            None => return,
                        };

                        let result = lvalue * rvalue;

                        out.store(result).await;
                    }
                }))
            }
            Expression::Div { left, right } => {
                let (mut left_in, left_out) = ctx.io();
                let (mut right_in, right_out) = ctx.io();

                pending.push((left, left_out));
                pending.push((right, right_out));

                Ok(Box::new(async move {
                    loop {
                        let lvalue = match left_in.get_value().await {
                            Some(v) => v,
                            None => return,
                        };
                        let rvalue = match right_in.get_value().await {
                            Some(v) => v,
                            None => return,
                        };

                        let result = lvalue / rvalue;

                        out.store(result).await;
                    }
                }))
            }
        }
    }
}

#[test]
fn constant_value() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let expr = Expression::Constant(13);

    rt.block_on(async move {
        let mut evm = vm::VM::construct::<MathInstruction>(&expr, &())
            .await
            .unwrap();

        assert_eq!(Some(13), evm.next().await);
        assert_eq!(None, evm.next().await);
    });
}

#[test]
fn addition() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let expr = Expression::Add {
        left: Box::new(Expression::Constant(13)),
        right: Box::new(Expression::Constant(14)),
    };
    rt.block_on(async move {
        let mut evm = vm::VM::construct::<MathInstruction>(&expr, &())
            .await
            .unwrap();

        assert_eq!(Some(27), evm.next().await);
        assert_eq!(None, evm.next().await);
    });
}

#[test]
fn repeating_value() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let expr = Expression::RepeatingValue(10);
    rt.block_on(async move {
        let mut evm = vm::VM::construct::<MathInstruction>(&expr, &())
            .await
            .unwrap();

        assert_eq!(Some(10), evm.next().await);
        assert_eq!(Some(10), evm.next().await);
    });
}
