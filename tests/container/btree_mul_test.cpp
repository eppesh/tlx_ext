/*******************************************************************************
 * tests/container/btree_test.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <random>
#include <set>
#include <string>
#include <tlx/container/btree_map.hpp>
#include <tlx/container/btree_multimap.hpp>
#include <tlx/container/btree_multiset.hpp>
#include <tlx/container/btree_set.hpp>
#include <tlx/die.hpp>
#include <utility>
#include <vector>

/******************************************************************************/
template <class KeyType>
std::vector<KeyType> ReadDataFromFile(const std::string& filename) {
    std::ifstream file(filename, std::ios::in);
    std::vector<KeyType> data;
    if (!file.is_open()) {
        std::cout << "Could not open file" << std::endl;
        return data;
    }
    std::string line;
    while (std::getline(file, line)) {
        data.push_back(std::stoi(line));
    }

    file.close();
    std::cout << "Read data from file successfully" << std::endl;
    return data;
}

template <class KeyType>
class TlxBTreeTest {
   public:
    TlxBTreeTest(const std::vector<KeyType>& data, int num_threads)
        : data_(data), num_threads_(num_threads) {}

    void RunWriteTest() {
        tlx::btree_map<KeyType, KeyType> btree;
        auto start = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> threads;
        for (int i = 0; i < num_threads_; ++i) {
            int chunk_size = data_.size() / num_threads_;
            int start_index = i * chunk_size;
            int end_index = (i == num_threads_ - 1) ? data_.size()
                                                    : start_index + chunk_size;
            threads.push_back(std::thread(WriteThreadTask, std::ref(btree),
                                          std::ref(data_), start_index,
                                          end_index));
        }

        for (auto& thread : threads) {
            thread.join();
        }

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        double throughput =
            data_.size() / duration.count() / 1024 / 1024;  // Mops/s

        std::cout << "Write throughput," << num_threads_ << "," << throughput
                  << ",Mops/s" << std::endl;
        std::cout << "Data size: " << data_.size()
                  << "; elpased time(s): " << duration.count() << std::endl;
    }

    static void WriteThreadTask(tlx::btree_map<KeyType, KeyType>& btree,
                                const std::vector<uint64_t>& data, int start,
                                int end) {
        for (int i = start; i < end; ++i) {
            btree.insert(std::make_pair(data[i], data[i]));
        }
    }

   private:
    const std::vector<KeyType>& data_;
    int num_threads_;
};

int main() {
    std::string filename = "/tmp/trace/fiu/FIU_iodedup_mail10.csv";
    std::cout << "Start multi-threaded test ..." << std::endl;
    std::cout << "Trace: " << filename << std::endl;
    std::vector<uint64_t> data = ReadDataFromFile<uint64_t>(filename);
    if (data.empty()) {
        std::cerr << "data is empty" << std::endl;
        return 0;
    }
    std::vector<int> thread_counts = {1};  // 1, 2, 4, 8
    for (int num_threads : thread_counts) {
        std::cout << "Number of threads: " << num_threads << std::endl;
        TlxBTreeTest<uint64_t> test(data, num_threads);
        test.RunWriteTest();
    }

    return 0;
}

/******************************************************************************/
