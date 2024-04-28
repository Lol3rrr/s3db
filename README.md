# S3DB
A toy experimental postgres compatible database using S3 as permanent storage

## Benchmarking
* Running the postgres benchmark: `python3 pgbench.py` (requires pgbench to be installed locally)
* Running rust benchmarks: `cargo bench`

## Loom Testing
`RUSTFLAGS="--cfg loom" cargo test --release`
