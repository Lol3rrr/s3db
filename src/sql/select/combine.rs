use nom::{IResult, Parser};

#[derive(Debug, PartialEq, Clone)]
pub enum Combination {
    Union,
    Intersection,
    Except,
}

pub fn combine(i: &[u8]) -> IResult<&[u8], Combination> {
    nom::branch::alt((
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("UNION"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("ALL"),
        ))
        .map(|_| Combination::Union),
        nom::bytes::complete::tag_no_case("UNION").map(|_| Combination::Union),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("INTERSECT"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("ALL"),
        ))
        .map(|_| Combination::Intersection),
        nom::bytes::complete::tag_no_case("INTERSECT").map(|_| Combination::Intersection),
        nom::sequence::tuple((
            nom::bytes::complete::tag_no_case("EXCEPT"),
            nom::character::complete::multispace1,
            nom::bytes::complete::tag_no_case("ALL"),
        ))
        .map(|_| Combination::Except),
        nom::bytes::complete::tag_no_case("EXCEPT").map(|_| Combination::Except),
    ))(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_combine() {
        let (remaining, test) = combine("UNION".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(Combination::Union, test);

        let (remaining, test) = combine("UNION ALL".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(Combination::Union, test);

        let (remaining, test) = combine("INTERSECT".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(Combination::Intersection, test);

        let (remaining, test) = combine("EXCEPT".as_bytes()).unwrap();
        assert_eq!(&[] as &[u8], remaining);
        assert_eq!(Combination::Except, test);
    }
}
