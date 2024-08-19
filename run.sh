#!/bin/bash

# Define the build command
BUILD_COMMAND="make tlx_container_btree_benchmarktest -j 16"

# Check if the build directory exists
if [ ! -d "build" ]; then    
    mkdir -p build
fi

cd build/tests
$BUILD_COMMAND

# Run './build/tests/tlx_container_btree_benchmarktest'
./tlx_container_btree_benchmarktest \
--tracefile=/tmp/trace/fiu/FIU_iodedup_mail10.csv \
--worker_threads=8 \
--bulk_load_ratio=0.5 \
--read_ratio=0 \
--insert_ratio=100 \
--benchmarks=readtrace,format,genmixworkload,bulkload,tlx_read_write \