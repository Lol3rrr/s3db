use divan::{AllocProfiler, Bencher};

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main();
}

#[divan::bench()]
fn simple_select(bencher: Bencher) {
    let query = "SELECT name FROM users";
    bencher.bench(|| divan::black_box(sql::Query::parse(query.as_bytes())));
}

#[divan::bench()]
fn select_join(bencher: Bencher) {
    let query = "SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.uid";
    bencher.bench(|| divan::black_box(sql::Query::parse(query.as_bytes())));
}

#[divan::bench()]
fn simple_insert(bencher: Bencher) {
    bencher.bench(|| {
        divan::black_box(sql::Query::parse(
            "INSERT INTO users(name) VALUES('testing')".as_bytes(),
        ))
    });
}
