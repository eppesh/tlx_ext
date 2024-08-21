#!/bin/bash

MODE=release

# Define the build command
CMAKE_CMD_D="cmake -DCMAKE_BUILD_TYPE=Debug -DTLX_BUILD_TESTS=ON .."
CMAKE_CMD_R="cmake -DCMAKE_BUILD_TYPE=Release -DTLX_BUILD_TESTS=ON .."
BUILD_CMD="make tlx_container_btree_benchmarktest -j 16"

echo "Run mode: $MODE"
if [ $MODE = "debug" ]; then
    PREFIX_CMD="gdb --args"
    BUILD_DIR=build
    # Check if the build directory exists
    if [ ! -d $BUILD_DIR ]; then
        mkdir -p $BUILD_DIR
    fi
    cd $BUILD_DIR
    $CMAKE_CMD_D
    echo $CMAKE_CMD_R
else
    PREFIX_CMD=""
    BUILD_DIR=buildr
    # Check if the build directory exists
    if [ ! -d $BUILD_DIR ]; then
        mkdir -p $BUILD_DIR
    fi
    cd $BUILD_DIR
    $CMAKE_CMD_R
    echo $CMAKE_CMD_R
fi

cd tests
$BUILD_CMD

# Run './$BUILD_DIR/tests/tlx_container_btree_benchmarktest'
$PREFIX_CMD ./tlx_container_btree_benchmarktest \
--tracefile=/tmp/trace/fiu/FIU_iodedup_mail10.csv \
--worker_threads=8 \
--bulk_load_ratio=0 \
--read_ratio=0 \
--insert_ratio=100 \
--benchmarks=readtrace,format,genmixworkload,bulkload,tlx_read_write,verify \
