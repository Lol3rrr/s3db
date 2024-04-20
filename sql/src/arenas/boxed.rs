#[allow(clippy::disallowed_types)]

#[derive(Debug)]
pub enum Boxed<'a, T> {
    Arena(bumpalo::boxed::Box<'a, T>),
    Heap(Box<T>),
}

impl<'a, T> Boxed<'a, T> {
    pub fn new(data: T) -> Boxed<'static, T> {
        Boxed::Heap(Box::new(data))
    }

    pub fn arena(a: &'a bumpalo::Bump, data: T) -> Self {
        Self::Arena(bumpalo::boxed::Box::new_in(data, a))
    }
    
    pub fn into_static(self) -> Boxed<'static, T> {
        match self {
            Self::Arena(bumped) => {
                Boxed::Heap(Box::new(bumpalo::boxed::Box::into_inner(bumped)))
            },
            Self::Heap(heaped) => Boxed::Heap(heaped),
        }
    }
    
    pub fn into_inner(value: Self) -> T {
        match value {
            Self::Heap(v) => *v,
            Self::Arena(v) => bumpalo::boxed::Box::into_inner(v),
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

impl<'a, T> AsRef<T> for Boxed<'a, T> {
    fn as_ref(&self) -> &T {
        match self {
            Self::Heap(v) => &v,
            Self::Arena(v) => &v,
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

impl<'a, T> crate::CompatibleParser for Boxed<'a, T> where T: crate::CompatibleParser {
    type StaticVersion = Boxed<'static, T::StaticVersion>;

    fn to_static(&self) -> Self::StaticVersion {
        match self {
            Self::Heap(v) => Boxed::Heap(Box::new(v.to_static())),
            Self::Arena(v) => Boxed::Heap(Box::new(v.to_static()))
        }
    }

    fn parameter_count(&self) -> usize {
        match self {
            Self::Heap(v) => v.parameter_count(),
            Self::Arena(v) => v.parameter_count(),
        }
    }
}
