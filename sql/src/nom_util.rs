pub fn bump_many0<'a, P, I, O, E>(
    arena: &'a bumpalo::Bump,
    mut parser: P,
) -> impl FnMut(I) -> nom::IResult<I, crate::arenas::Vec<'a, O>, E>
where
    P: nom::Parser<I, O, E>,
    I: Copy,
{
    move |i| {
        let mut result = bumpalo::collections::Vec::new_in(arena);
        let mut remaining = i;

        loop {
            match nom::Parser::parse(&mut parser, remaining) {
                Ok((rem_tmp, res_tmp)) => {
                    result.push(res_tmp);
                    remaining = rem_tmp;
                }
                Err(_) => return Ok((remaining, crate::arenas::Vec::Arena(result))),
            };
        }
    }
}

pub fn bump_separated_list0<'a, S, P, I, O, SO, E>(arena: &'a bumpalo::Bump, mut seperator: S, mut parser: P) -> impl FnMut(I) -> nom::IResult<I, crate::arenas::Vec<'a, O>, E>
where
    P: nom::Parser<I, O, E>,
    S: nom::Parser<I, SO, E>,
    I: Copy {
    move |i| {
        let mut result = bumpalo::collections::Vec::new_in(arena);

        let mut remaining = match nom::Parser::parse(&mut parser, i) {
            Ok((remaining, first)) => {
                result.push(first);
                remaining
            }
            Err(_) => return Ok((i, crate::arenas::Vec::Arena(result))),
        };

        loop {
            match nom::Parser::parse(&mut seperator, remaining) {
                Ok((rem, _)) => {
                    remaining = rem;
                }
                Err(_) => return Ok((remaining, crate::arenas::Vec::Arena(result))),
            };

            match nom::Parser::parse(&mut parser, remaining) {
                Ok((n_rem, entry)) => {
                    result.push(entry);
                    remaining = n_rem;
                }
                Err(_) => return Ok((remaining, crate::arenas::Vec::Arena(result)))
            };
        }
    }
}

pub fn bump_separated_list1<'a, S, P, I, O, SO, E>(arena: &'a bumpalo::Bump, mut seperator: S, mut parser: P) -> impl FnMut(I) -> nom::IResult<I, crate::arenas::Vec<'a, O>, E>
where
    P: nom::Parser<I, O, E>,
    S: nom::Parser<I, SO, E>,
    I: Copy {
    move |i| {
        let mut result = bumpalo::collections::Vec::new_in(arena);

        let mut remaining = match nom::Parser::parse(&mut parser, i) {
            Ok((remaining, first)) => {
                result.push(first);
                remaining
            }
            Err(e) => return Err(e),
        };

        loop {
            match nom::Parser::parse(&mut seperator, remaining) {
                Ok((rem, _)) => {
                    remaining = rem;
                }
                Err(_) => return Ok((remaining, crate::arenas::Vec::Arena(result))),
            };

            match nom::Parser::parse(&mut parser, remaining) {
                Ok((n_rem, entry)) => {
                    result.push(entry);
                    remaining = n_rem;
                }
                Err(_) => return Ok((remaining, crate::arenas::Vec::Arena(result)))
            };
        }
    }
}
