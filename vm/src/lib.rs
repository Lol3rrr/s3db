mod io;
pub use io::*;

/// This trait describes the requirements for being able to run an Instruction on the [`VM`]
pub trait Instruction<'i> {
    type ConstructError;
    type Input;

    /// The Value that the Instructions will output as a result of being evaluated
    type Value;

    /// Allow for arguments to be passed from the Outside
    type Arguments;

    /// This should first add all it's dependencies onto the provided `pending` Vec, so that they
    /// will be processed as well, and then return a boxed [Future](core::future::Future) which will 
    /// do the actual processing for the given input instruction.
    ///
    /// # General Hints
    /// If the future is just a constant, it can just write the result to the provided output and
    /// then exit. However if the future is kind of like a `map` or `filter`, you will likely want
    /// to run it as a loop, which will read from it's input and then write to the output in every
    /// iteration
    fn construct<'p>(
        input: &'i Self::Input,
        pending: &'p mut Vec<(&'i Self::Input, Output<Self::Value>)>,
        out: Output<Self::Value>,
        ctx: &mut ConstructionContext<Self::Value>,
        args: &Self::Arguments,
    ) -> impl core::future::Future<Output = Result<Box<dyn core::future::Future<Output = ()> + 'i>, Self::ConstructError>>;
}

struct RuntimeState {
    current_fut: core::sync::atomic::AtomicUsize,
    ignore_pending: core::sync::atomic::AtomicBool,
}

/// A "VM" that can execute Instructions to generate one more values.
pub struct VM<'i, V> {
    root_io: Input<V>,
    instructions: Vec<*mut (dyn core::future::Future<Output = ()> + 'i)>,
    rt_state: std::rc::Rc<RuntimeState>,
    _marker: core::marker::PhantomData<&'i ()>,
}

#[doc(hidden)]
pub struct VMGetNextFuture<'r, 'i, V> {
    vm_: &'r mut VM<'i, V>,
}

impl<'i, V> VM<'i, V> {
    pub async fn construct<I>(input: &'i I::Input, args: &I::Arguments) -> Result<Self, I::ConstructError>
    where
        I: Instruction<'i, Value = V>,
    {
        let fut_ptr = std::rc::Rc::new(RuntimeState {
            current_fut: core::sync::atomic::AtomicUsize::new(0),
            ignore_pending: core::sync::atomic::AtomicBool::new(false),
        });

        let mut context = ConstructionContext::new(fut_ptr.clone());
        let (root_in, root_out) = context.io();

        let mut pending: Vec<(&'i <I as Instruction>::Input, Output<I::Value>)> =
            vec![(input, root_out)];
        let mut instructions: Vec<*mut (dyn core::future::Future<Output = ()> + 'i)> = Vec::new();
        while let Some((tmp, output)) = pending.pop() {
            let io_inner = output.data.clone();

            let fut = I::construct(tmp, &mut pending, output, &mut context, args).await?;
            let instr_fut = Box::into_raw(fut);

            let idx = instructions.len();
            for io in context.created_ios.drain(..) {
                io.register_consumer(idx);
            }
            io_inner.register_producer(idx);
            instructions.push(instr_fut);
        }

        Ok(Self {
            root_io: root_in,
            rt_state: fut_ptr,
            instructions,
            _marker: core::marker::PhantomData {},
        })
    }

    pub fn get_next(&mut self) -> VMGetNextFuture<'_, 'i, V> {
        VMGetNextFuture {
            vm_: self,
        }
    }

    fn inner_poll(&mut self, cx: &mut std::task::Context<'_>,) -> core::task::Poll<Option<V>> {
        loop {
            let idx = self
                .rt_state.current_fut
                .load(core::sync::atomic::Ordering::Acquire);

            if idx == 0 && self.root_io.is_done() {
                let res = self.root_io.try_get();
                return core::task::Poll::Ready(res);
            }

            let fut_ptr = self.instructions.get_mut(idx).copied().unwrap();
            let fut_ref: &mut (dyn core::future::Future<Output = ()> + 'i) =
                unsafe { &mut *fut_ptr };
            let pinned = unsafe { core::pin::Pin::new_unchecked(fut_ref) };

            match pinned.poll(cx) {
                core::task::Poll::Pending if !self.rt_state.ignore_pending.load(core::sync::atomic::Ordering::Acquire)  => return core::task::Poll::Pending,
                _ => {
                    self.rt_state.ignore_pending.store(false, core::sync::atomic::Ordering::Release);
                    if idx == 0 {
                        self.rt_state
                            .current_fut
                            .store(0, core::sync::atomic::Ordering::Release);
                        return core::task::Poll::Ready(self.root_io.try_get());
                    }
                    continue;
                }
            }
        }
    }
}

impl<'i, V> Drop for VM<'i, V> {
    fn drop(&mut self) {
        for instr_ptr in self.instructions.drain(..) {
            let boxed = unsafe { Box::from_raw(instr_ptr) };
            drop(boxed);
        }
    }
}

impl<'i, V> futures::stream::Stream for VM<'i, V> {
    type Item = V;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let inner = self.get_mut();
        Self::inner_poll(inner, cx)
    }
}

impl<'r, 'i, V> core::future::Future for VMGetNextFuture<'r, 'i, V> {
    type Output = Option<V>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> core::task::Poll<Option<V>> {
        let inner = self.get_mut();
        VM::inner_poll(&mut inner.vm_, cx)
    }
}
