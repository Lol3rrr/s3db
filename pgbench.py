import subprocess
import time
import signal
import sys
import argparse

parser = argparse.ArgumentParser(description="Run pgbench benchmarks")
parser.add_argument("--metrics", action="store_true")
parser.add_argument("--repo-path", default="./")

args = parser.parse_args()

def parse_metrics(raw: str):
    metrics = {}

    print(f"Raw: \n{raw}")
    for line in raw.splitlines():
        if line.startswith("tps = "):
            rest = line.removeprefix("tps = ")
            rest = rest.removesuffix(" (without initial connection time)")
            tps = float(rest)
            metrics["tps"] = tps
        else:
            print(f"Unknown Line: {line}")

    return metrics

print(f"Compiling S3DB in release mode", flush=True)
build_res = subprocess.run(["cargo", "build", "--release"], capture_output=True, cwd=args.repo_path)
if build_res.returncode != 0:
    sys.exit(-1)

print(f"Starting database", flush=True)
s3db_process = subprocess.Popen(["target/release/s3db"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=args.repo_path)

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

s3db_process.kill()

if args.metrics:
    bench_results = parse_metrics(output)
    with open("metrics.json", mode='w') as f:
        f.write(f"{{\"pgbench/tpc-b\": {{ \"throughput\": {{ \"value\": { bench_results['tps'] } }} }}}}\n")
