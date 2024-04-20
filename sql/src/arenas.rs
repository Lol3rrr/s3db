#![allow(clippy::disallowed_types)]

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

    pub fn push(&mut self, data: T) {
        match self {
            Self::Arena(v) => {
                v.push(data);
            }
            Self::Heap(v) => {
                v.push(data);
            }
        };
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

impl<'a, T> IntoIterator for Vec<'a, T> {
    type IntoIter = ArenaVecIterator<'a, T>;
    type Item = T;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Heap(h) => ArenaVecIterator::Heap(h.into_iter()),
            Self::Arena(a) => ArenaVecIterator::Arena(a.into_iter()),
        }
    }
}

impl<'r,'a, T> IntoIterator for &'r Vec<'a, T> {
    type Item = &'r T;
    type IntoIter = core::slice::Iter<'r, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub enum ArenaVecIterator<'a, T> {
    Heap(std::vec::IntoIter<T>),
    Arena(bumpalo::collections::vec::IntoIter<'a, T>),
}

impl<'a, T> Iterator for ArenaVecIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Heap(iter) => iter.next(),
            Self::Arena(iter) => iter.next(),
        }
    }
}

impl<'a, T> crate::CompatibleParser for Vec<'a, T> where T: crate::CompatibleParser {
    type StaticVersion = Vec<'static, T::StaticVersion>;

    fn to_static(&self) -> Self::StaticVersion {
        Vec::Heap(self.iter().map(|v| v.to_static()).collect())
    }

    fn parameter_count(&self) -> usize {
        self.iter().map(|v| v.parameter_count()).max().unwrap_or(0)
    }
}
