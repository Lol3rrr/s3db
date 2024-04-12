use super::CTE;

#[derive(Debug)]
pub struct ParsingContext {
    pub(super) ctes: Vec<CTE>,
}

impl Default for ParsingContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ParsingContext {
    pub fn new() -> Self {
        Self { ctes: Vec::new() }
    }

    pub fn add_cte(&mut self, cte: CTE) {
        self.ctes.push(cte);
    }
}
