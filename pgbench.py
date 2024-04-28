import subprocess
import time
import signal
import sys

print(f"Compiling S3DB in release mode", flush=True)
build_res = subprocess.run(["cargo", "build", "--release"], capture_output=True)
if build_res.returncode != 0:
    sys.exit(-1)

print(f"Starting database", flush=True)
s3db_process = subprocess.Popen(["target/release/s3db"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

print(f"Waiting 10 Seconds for database to get up and running", flush=True)
time.sleep(10)

if s3db_process.poll() is not None:
    raise ValueError("S3DB process stopped", flush=True)

bench_duration = 30
if subprocess.run(["pgbench", "-h", "localhost", "-i", "-n"]).resultcode != 0:
    sys.exit(-1)
subprocess.run(["pgbench", "-h", "localhost", "-n", "-T", f"{bench_duration}", "--progress", "1"])

s3db_process.kill()
