use sql::DataType;

#[derive(Debug, PartialEq, Clone)]
pub struct PossibleTypes {
    pos: Vec<DataType>,
}

impl PossibleTypes {
    pub fn all() -> Self {
        let types = vec![
            DataType::Name,
            DataType::Serial,
            DataType::BigSerial,
            DataType::Text,
            DataType::Bool,
            DataType::Timestamp,
            DataType::SmallInteger,
            DataType::Integer,
            DataType::BigInteger,
            DataType::ByteA,
            DataType::DoublePrecision,
            DataType::Real,
        ];

        // The -2 is here because we cant model the varchar and char types
        debug_assert_eq!(
            core::mem::variant_count::<DataType>() - 2,
            types.len(),
            "There are types missing from the all possible types option"
        );

        Self { pos: types }
    }

    pub fn fixed(ty: DataType) -> Self {
        Self { pos: vec![ty] }
    }

    pub fn specific<II>(tys: II) -> Self
    where
        II: IntoIterator<Item = DataType>,
    {
        Self {
            pos: tys.into_iter().collect(),
        }
    }

    pub fn fixed_with_conversions(ty: DataType) -> Self {
        match ty {
            DataType::Text => Self {
                pos: vec![
                    DataType::Text,
                    DataType::Name,
                    DataType::BigInteger,
                    DataType::Integer,
                    DataType::SmallInteger,
                ],
            },
            DataType::Name => Self {
                pos: vec![DataType::Name],
            },
            DataType::Bool => Self {
                pos: vec![DataType::Bool],
            },
            DataType::Serial => Self {
                pos: vec![
                    DataType::Serial,
                    DataType::Integer,
                    DataType::BigInteger,
                    DataType::Text,
                ],
            },
            DataType::BigSerial => Self {
                pos: vec![DataType::BigSerial, DataType::BigInteger, DataType::Text],
            },
            DataType::VarChar { size } => Self {
                pos: vec![DataType::VarChar { size }, DataType::Text],
            },
            DataType::Char { size } => Self {
                pos: vec![DataType::Char { size }, DataType::Text],
            },
            DataType::Timestamp => Self {
                pos: vec![DataType::Timestamp],
            },
            DataType::SmallInteger => Self {
                pos: vec![
                    DataType::SmallInteger,
                    DataType::Integer,
                    DataType::BigInteger,
                    DataType::Text,
                ],
            },
            DataType::Integer => Self {
                pos: vec![DataType::Integer, DataType::BigInteger, DataType::Text],
            },
            DataType::BigInteger => Self {
                pos: vec![DataType::BigInteger, DataType::Text],
            },
            DataType::ByteA => Self {
                pos: vec![DataType::ByteA],
            },
            DataType::DoublePrecision => Self {
                pos: vec![DataType::DoublePrecision],
            },
            DataType::Real => Self {
                pos: vec![DataType::Real],
            },
        }
    }

    pub fn compatible(&self, other: &Self) -> Self {
        Self {
            pos: self
                .pos
                .iter()
                .filter(|ty| other.pos.contains(ty))
                .cloned()
                .collect(),
        }
    }

    pub fn resolve(mut self) -> Option<DataType> {
        if self.pos.is_empty() {
            return None;
        }

        Some(self.pos.swap_remove(0))
    }

    pub fn from_data(data: &storage::Data) -> Self {
        match data {
            storage::Data::Null => Self::all(),
            other => todo!("Possible Types for {:?}", other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn any_fixed_compatible() {
        let all = PossibleTypes::all();
        let fixed = PossibleTypes::fixed(DataType::Text);

        let compatible = all.compatible(&fixed);

        assert_eq!(
            PossibleTypes {
                pos: vec![DataType::Text]
            },
            compatible
        );
    }
}
