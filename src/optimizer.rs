use crate::ra::RaExpression;

mod noop;
pub use noop::Noop;

mod conditions;

pub trait Optimize {
    fn optimize(&self, expr: RaExpression) -> RaExpression;
}
