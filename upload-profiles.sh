#!/bin/bash

set -e;

echo "Uploading Profiles"

BRANCH=$(git symbolic-ref --short HEAD)
COMMIT=$(git rev-parse HEAD)

root_benchmarks=target/criterion
for bench_harness in "$root_benchmarks"/*
do
  harnessname=$(basename "$bench_harness")
  echo "  Harness: $harnessname"
  for benchmark in "$bench_harness"/*
  do
    benchname=$(basename "$benchmark")
    echo "    Benchmark: $benchname"

    for param in "$benchmark"/*
    do
      paramname=$(basename "$param")
      echo "      Param: $paramname"

      profilepath="$param"/profile/profile.pb
      profilecli upload \
        --extra-labels=service_name=s3db \
        --extra-labels=harness="$harnessname" \
        --extra-labels=benchmark="$benchname" \
        --extra-labels=param="$paramname" \
        --extra-labels=branch="$BRANCH" \
        --extra-labels=commit="$COMMIT" \
        "$profilepath"
    done
  done
done
