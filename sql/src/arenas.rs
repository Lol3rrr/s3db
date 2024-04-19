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

#[derive(Debug)]
pub enum Vec<'a, T> {
    Arena(bumpalo::collections::Vec<'a, T>),
    Heap(std::vec::Vec<T>),
}

impl<'a, T> Vec<'a, T> {
    pub fn arena(a: &'a bumpalo::Bump) -> Self {
        Self::Arena(bumpalo::collections::Vec::new_in(a))
    }

    pub fn clone_to_heap(&self) -> Vec<'static, T> where T: Clone {
        let mut result = std::vec::Vec::with_capacity(self.len());
        result.extend(self.iter().cloned());
        Vec::Heap(result)
    }

    pub fn to_static(self) -> Vec<'static, T> {
        match self {
            Self::Arena(bumped) => Vec::Heap(bumped.into_iter().collect::<std::vec::Vec<_>>()),
            Self::Heap(heaped) => Vec::Heap(heaped),
        }
    }
}

impl<'a, T> From<bumpalo::collections::Vec<'a, T>> for Vec<'a, T> {
    fn from(value: bumpalo::collections::Vec<'a, T>) -> Self {
        Self::Arena(value)
    }
}
impl<'a, T> From<std::vec::Vec<T>> for Vec<'a, T> {
    fn from(value: std::vec::Vec<T>) -> Self {
        Self::Heap(value)
    }
}

impl<'a, T> core::ops::Deref for Vec<'a, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Arena(a) => a.deref(),
            Self::Heap(h) => h.deref(),
        }
    }
}

impl<'a, 'b, X, Y> PartialEq<Vec<'b, Y>> for Vec<'a, X> where X: PartialEq<Y> {
    fn eq(&self, other: &Vec<'b, Y>) -> bool {
        let first: &[X] = self;
        let second: &[Y] = other;
        first.eq(second)
    }
}
