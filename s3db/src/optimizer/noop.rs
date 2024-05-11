use ra::RaExpression;

use super::Optimize;

pub struct Noop {}

impl Optimize for Noop {
    fn optimize(&self, expr: RaExpression) -> RaExpression {
        expr
    }
}
