pub(super) struct IOData<V> {
    done: core::sync::atomic::AtomicBool,
    value: core::cell::UnsafeCell<Option<V>>,
    producer: core::sync::atomic::AtomicUsize,
    consumer: core::sync::atomic::AtomicUsize,
    runtime_ptr: std::rc::Rc<super::RuntimeState>,
}

/// An [`Input`] node, provides a way to receive the output from a different Instruction, which
/// will output it's value to the connected/corresponding [`Output`] node.
pub struct Input<V> {
    data: std::rc::Rc<IOData<V>>,
}

/// An [`Output`] node, which provides a way to output a produced value from an Instruction
pub struct Output<V> {
    pub(super) data: std::rc::Rc<IOData<V>>,
}

/// The Context for constructing the VM's instructions
pub struct ConstructionContext<V> {
    rt_ptr: std::rc::Rc<super::RuntimeState>,
    pub(super) created_ios: Vec<std::rc::Rc<IOData<V>>>,
}

impl<V> ConstructionContext<V> {
    pub(super) fn new(rt_ptr: std::rc::Rc<super::RuntimeState>) -> Self {
        Self {
            rt_ptr,
            created_ios: Vec::new(),
        }
    }

    /// Creates an IO pair, which should be used to connect two different instructions together
    ///
    /// The [`Input`] node, will be able to receive the values, produced/outputed using the returned
    /// [`Output`] node
    pub fn io(&mut self) -> (Input<V>, Output<V>) {
        let io_data = IOData {
            done: core::sync::atomic::AtomicBool::new(false),
            value: core::cell::UnsafeCell::new(None),
            producer: core::sync::atomic::AtomicUsize::new(usize::MAX),
            consumer: core::sync::atomic::AtomicUsize::new(usize::MAX),
            runtime_ptr: self.rt_ptr.clone(),
        };

        let rced = std::rc::Rc::new(io_data);

        self.created_ios.push(rced.clone());

        (Input { data: rced.clone() }, Output { data: rced })
    }
}

impl<V> Input<V> {
    /// Attempts to load a value for this input or otherwise schedule the instruction to produce a
    /// value for this.
    ///
    /// # Behaviour
    /// 1. If there is already a value stored for this input, this will return the value immediately
    ///    and take out the value stored.
    /// 2. If there is no value stored for this input and the instruction, that produces the value
    ///    for this, is done. it will return [`None`] immediately and also not do anything else
    /// 3. If there is no value stored, but the instruction, that produces is not yet done, that
    ///    instruction will be scheduled next and this future will resolve once that instruction
    ///    has produced an output
    pub fn get_value(&mut self) -> InputGetValueFuture<'_, V> {
        InputGetValueFuture { input: self }
    }

    /// Tries to load a Value stored for this input. If a value exists, that will be taken out and
    /// returned, otherwise simply returns [`None`]
    pub fn try_get(&mut self) -> Option<V> {
        let inner_data = unsafe { &mut *self.data.value.get() };
        inner_data.take()
    }

    /// Checks if the Instruction that is responsible for generating the value is done and will no
    /// longer produce any more values. Notably even if this method returns `true`, there might
    /// still be a value stored, which should be retrieved
    pub fn is_done(&self) -> bool {
        self.data.done.load(core::sync::atomic::Ordering::Acquire)
    }
}

#[doc(hidden)]
pub struct InputGetValueFuture<'i, V> {
    input: &'i mut Input<V>,
}

impl<'i, V> core::future::Future for InputGetValueFuture<'i, V> {
    type Output = Option<V>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner_data = unsafe { &mut *self.input.data.value.get() };
        match inner_data.take() {
            Some(d) => core::task::Poll::Ready(Some(d)),
            None => {
                if self
                    .input
                    .data
                    .done
                    .load(core::sync::atomic::Ordering::Acquire)
                {
                    return core::task::Poll::Ready(None);
                }

                let prod = self
                    .input
                    .data
                    .producer
                    .load(core::sync::atomic::Ordering::Acquire);
                self.input
                    .data
                    .runtime_ptr
                    .current_fut
                    .store(prod, core::sync::atomic::Ordering::Release);

                cx.waker().wake_by_ref();

                core::task::Poll::Pending
            }
        }
    }
}

impl<V> Output<V> {
    /// Stores the value to make it accessable to the conncted [`Input`] and also schedules the
    /// Instruction that uses the [`Input`] to be executed next
    pub fn store(&mut self, value: V) -> OutputStoreValueFuture<'_, V> {
        OutputStoreValueFuture {
            output: self,
            value: Some(value),
        }
    }

    /// This will close the [`Output`], which indicates to the [`Input`] that there will be no new
    /// values generated from this instruction. This will automatically happen if the [`Output`] is
    /// dropped, but can also be done manually by calling this function.
    pub fn done(&mut self) {
        self.data
            .done
            .store(true, core::sync::atomic::Ordering::Release);
        let consumer = self
            .data
            .consumer
            .load(core::sync::atomic::Ordering::Acquire);
        self.data
            .runtime_ptr
            .current_fut
            .store(consumer, core::sync::atomic::Ordering::Release);
    }
}
impl<V> Drop for Output<V> {
    fn drop(&mut self) {
        self.done();
    }
}

#[doc(hidden)]
pub struct OutputStoreValueFuture<'o, V> {
    output: &'o mut Output<V>,
    value: Option<V>,
}

impl<'o, V> core::future::Future for OutputStoreValueFuture<'o, V>
where
    V: Unpin,
{
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let self_mut = self.get_mut();

        match self_mut.value.take() {
            Some(v) => {
                let v_ptr = self_mut.output.data.value.get();
                let _ = core::mem::replace(unsafe { &mut *v_ptr }, Some(v));

                let consumer = self_mut
                    .output
                    .data
                    .consumer
                    .load(core::sync::atomic::Ordering::Acquire);
                self_mut
                    .output
                    .data
                    .runtime_ptr
                    .current_fut
                    .store(consumer, core::sync::atomic::Ordering::Release);

                self_mut
                    .output
                    .data
                    .runtime_ptr
                    .ignore_pending
                    .store(true, core::sync::atomic::Ordering::Release);

                core::task::Poll::Pending
            }
            None => core::task::Poll::Ready(()),
        }
    }
}

impl<V> IOData<V> {
    pub fn register_producer(&self, producer: usize) {
        self.producer
            .store(producer, core::sync::atomic::Ordering::Release);
    }
    pub fn register_consumer(&self, consumer: usize) {
        self.consumer
            .store(consumer, core::sync::atomic::Ordering::Release);
    }
}
