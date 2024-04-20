#[allow(clippy::disallowed_types)]

#[derive(Debug)]
pub enum Boxed<'a, T> {
    Arena(bumpalo::boxed::Box<'a, T>),
    Heap(Box<T>),
}

impl<'a, T> Boxed<'a, T> {
    pub fn arena(a: &'a bumpalo::Bump, data: T) -> Self {
        Self::Arena(bumpalo::boxed::Box::new_in(data, a))
    }
    
    pub fn to_static(self) -> Boxed<'static, T> {
        match self {
            Self::Arena(bumped) => {
                Boxed::Heap(Box::new(bumpalo::boxed::Box::into_inner(bumped)))
            },
            Self::Heap(heaped) => Boxed::Heap(heaped),
        }
    } 
}

impl<T> From<std::boxed::Box<T>> for Boxed<'static, T> {
    fn from(value: std::boxed::Box<T>) -> Self {
        Self::Heap(value)
    }
}

impl<'a, T> core::ops::Deref for Boxed<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Arena(v) => v,
            Self::Heap(v) => v,
        }
    }
}

impl<'a, 'b, X, Y> PartialEq<Boxed<'b, Y>> for Boxed<'a, X> where X: PartialEq<Y> {
    fn eq(&self, other: &Boxed<'b, Y>) -> bool {
        let first: &X = self;
        let second: &Y = other;
        first == second
    }
}
