pub fn bump_many0<'a, P, I, O, E>(
    arena: &'a bumpalo::Bump,
    mut parser: P,
) -> impl FnMut(I) -> nom::IResult<I, bumpalo::collections::Vec<'a, O>, E>
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
                Err(_) => return Ok((remaining, result)),
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
