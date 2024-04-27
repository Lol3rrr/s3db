import subprocess
import time

print(f"Compiling S3DB in release mode")
build_res = subprocess.run(["cargo", "build", "--release"], capture_output=True)

print(f"Starting database")
s3db_process = subprocess.Popen(["target/release/s3db"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

print(f"Waiting 10 Seconds for database to get up and running")
time.sleep(10)

subprocess.run(["pgbench", "-h", "localhost", "-i"])
subprocess.run(["pgbench", "-h", "localhost", "-T", "30", "-S", "--progress", "1"])
