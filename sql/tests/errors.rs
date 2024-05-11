macro_rules! parse_error {
    ($content:literal) => {{
        let arena = bumpalo::Bump::new();
        match ::sql::Query::parse($content.as_bytes(), &arena) {
            Ok(v) => panic!("Parsing returned Ok: {:?}", v),
            Err(_) => {},
        };
    }}
}

#[test]
fn select_trailing_comma() {
    parse_error!("SELECT first, second, FROM testing");
}

#[test]
fn select_no_fields() {
    parse_error!("SELECT FROM testing");
}
