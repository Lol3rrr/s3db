import subprocess
import time
import signal
import sys
import argparse
import json
import os

parser = argparse.ArgumentParser(description="Run pgbench benchmarks")
parser.add_argument("--metrics", action="store_true")
parser.add_argument("--repo-path", default="./")
parser.add_argument("--profile", action="store_true")

args = parser.parse_args()

def parse_metrics(raw: str, benchname: str):
    metrics = {}

    print(f"Raw: \n{raw}")
    for line in raw.splitlines():
        if line.startswith("tps = "):
            rest = line.removeprefix("tps = ")
            rest = rest.removesuffix(" (without initial connection time)")
            tps = float(rest)
            metrics["throughput"] = { "value": tps }
        else:
            print(f"Unknown Line: {line}")

    return { benchname: metrics }

print(f"Compiling S3DB in release mode", flush=True)

build_env = os.environ.copy()
if args.profile:
    build_env["RUSTFLAGS"] = "--cfg profiling"

build_res = subprocess.run(["cargo", "build", "--release"], capture_output=True, cwd=args.repo_path, env=build_env)
if build_res.returncode != 0:
    print(build_res.stderr)
    sys.exit(-1)

print(f"Starting database", flush=True)
s3db_file = open("s3db.std.log", 'w')
s3db_process = subprocess.Popen(["target/release/s3db"], stdout=s3db_file, stderr=s3db_file, cwd=args.repo_path)

print(f"Waiting 5 Seconds for database to get up and running", flush=True)
time.sleep(5)

if s3db_process.poll() is not None:
    raise ValueError("S3DB process stopped")

bench_duration = 10
if subprocess.run(["pgbench", "-h", "localhost", "-i", "-n"]).returncode != 0:
    sys.exit(-1)

output = ""
with subprocess.Popen(["pgbench", "-h", "localhost", "-n", "-T", f"{bench_duration}", "--progress", "1"], stdout=subprocess.PIPE) as p:
    while p.poll() is None:
        text = p.stdout.read1().decode('utf-8')
        output = output + text
        print(text, end='', flush=True)

s3db_process.terminate()

if args.metrics:
    bench_results = parse_metrics(output, "pgbench/tpc-b")
    with open("metrics.json", mode='w') as f:
        json.dump(bench_results, f)
