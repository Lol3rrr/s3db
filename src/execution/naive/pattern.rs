#[derive(Debug, PartialEq)]
enum Pattern<'p> {
    Literal(&'p str),
    AnySingle,
    AnyChars,
}
fn match_pattern(patterns: &[Pattern<'_>], haystack: &str) -> bool {
    let pattern = match patterns.get(0) {
        Some(p) => p,
        None => return haystack.is_empty(),
    };

    match pattern {
        Pattern::Literal(lit_val) => {
            if !haystack.starts_with(lit_val) {
                return false;
            }

            match_pattern(&patterns[1..], &haystack[lit_val.len()..])
        }
        Pattern::AnySingle => {
            if haystack.is_empty() {
                return false;
            }

            match_pattern(&patterns[1..], &haystack[1..])
        }
        Pattern::AnyChars => {
            for offset in 0..haystack.len() + 1 {
                if match_pattern(&patterns[1..], &haystack[offset..]) {
                    return true;
                }
            }

            false
        }
    }
}

pub fn like_match(haystack: &str, pattern: &str) -> bool {
    let parts: Vec<Pattern<'_>> = {
        let mut tmp = Vec::new();

        let mut start: usize = 0;
        for (i, entry) in pattern.char_indices() {
            if entry == '%' {
                let literal = &pattern[start..i];
                tmp.push(Pattern::Literal(literal));

                tmp.push(Pattern::AnyChars);
                start = i + 1;
            } else if entry == '_' {
                let literal = &pattern[start..i];
                tmp.push(Pattern::Literal(literal));

                tmp.push(Pattern::AnySingle);
                start = i + 1;
            }
        }

        if start < pattern.len() {
            let literal = &pattern[start..];
            tmp.push(Pattern::Literal(literal));
        }

        tmp
    };

    match_pattern(&parts, haystack)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_special() {
        assert!(!like_match("first", "second"));
        assert!(like_match("first", "first"));
    }

    #[test]
    fn single_any_character() {
        assert!(like_match("first", "firs_"));
        assert!(like_match("firsx", "firs_"));
        assert!(!like_match("firsts", "firs_"));
    }

    #[test]
    fn end_any_charachers() {
        assert!(like_match("first", "firs%"));
        assert!(like_match("firsx", "firs%"));
        assert!(like_match("firsts", "firs%"));
    }
}
