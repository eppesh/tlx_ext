#pragma once

#include <sys/time.h>
/* #include <tbb/blocked_range.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_sort.h> */

#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Returns the number of micro-seconds since some fixed point in time. Only
// useful for computing deltas of time.
inline uint64_t NowMicros() {
    static constexpr uint64_t kUsecondsPerSecond = 1000000;
    struct ::timeval tv;
    ::gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * kUsecondsPerSecond + tv.tv_usec;
}
// Returns the number of nano-seconds since some fixed point in time. Only
// useful for computing deltas of time in one run.
// Default implementation simply relies on NowMicros.
// In platform-specific implementations, NowNanos() should return time points
// that are MONOTONIC.
inline uint64_t NowNanos() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000L + ts.tv_nsec;
}

static constexpr uint64_t kRandNumMax = (1LU << 60);
static constexpr uint64_t kRandNumMaxMask = kRandNumMax - 1;

static uint64_t u64Rand(const uint64_t& min, const uint64_t& max) {
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distribution(min, max);
    return distribution(generator);
}

enum Operation { kRead = 0, kInsert, kUpdate, kDelete, kScan };
template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class RandomKeyTrace {
   public:
    RandomKeyTrace(size_t count, bool is_seq = false,
                   bool prepared_trace = false) {
        if (prepared_trace) {
            count_ = count;
            unique_ordered_keys_.resize(count);

            printf("generate %lu keys, is_seq %d\n", count, is_seq);
            auto starttime = std::chrono::system_clock::now();
            for (size_t i = 0; i < count; i++) {
                unique_ordered_keys_[i] = i + 1;
            }

            printf("Ensure there is no duplicate keys\n");
            std::sort(unique_ordered_keys_.begin(), unique_ordered_keys_.end());
            auto it = std::unique(unique_ordered_keys_.begin(),
                                  unique_ordered_keys_.end());
            unique_ordered_keys_.erase(it, unique_ordered_keys_.end());
            printf("%lu keys left \n", unique_ordered_keys_.size());

            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("generate duration %f s.\n", duration.count() / 1000000.0);

            size_t element_memory =
                unique_ordered_keys_.size() * sizeof(uint64_t);

            printf("key space usage %2.10f GB\n",
                   element_memory / 1024.0 / 1024 / 1024);
        }
    }

    /* void prepareBuckloadKeys () {
        bulkload_keys_ = new std::pair<KEY_TYPE,
    PAYLOAD_TYPE>[unique_ordered_keys_.size ()]; for (int i = 0; i <
    unique_ordered_keys_.size (); i++) { bulkload_keys_[i].first =
    unique_ordered_keys_[i]; bulkload_keys_[i].second = 123456789;
        }
    } */

    void ToFile(const std::string& filename) {
        std::ofstream outfile(filename, std::ios::out | std::ios_base::binary);
        for (size_t k : unique_ordered_keys_) {
            outfile.write(reinterpret_cast<char*>(&k), 8);
            // printf ("out key %016lu\n", k);
        }
        outfile.close();
    }

    // Check if a string is empty or consists only of whitespace characters
    bool IsWhitespaceOnly(const std::string& str) {
        return str.empty() || std::all_of(str.begin(), str.end(), isspace);
    }

    bool FromTextFile(const std::string& filename) {
        std::ifstream infile(filename, std::ios::in);
        if (!infile.is_open()) {
            std::cout << "[FromTextFile] Error opening " << filename
                      << std::endl;
            return false;
        }
        std::string line = "";
        unique_ordered_keys_.clear();
        while (std::getline(infile, line)) {
            std::istringstream iss(line);
            if (IsWhitespaceOnly(line)) {
                continue;
            }
            KEY_TYPE key;
            iss >> key;
            unique_ordered_keys_.push_back(key);
            read_keys_.push_back(key);
        }
        count_ = unique_ordered_keys_.size();
        infile.close();
        return true;
    }

    bool ReadFiuTrace(const std::string& filename) {
        std::ifstream infile(filename, std::ios::in);
        if (!infile.is_open()) {
            std::cout << "[ReadFiuTrace] Error opening file: " << filename
                      << std::endl;
            return false;
        }
        std::string line = "";
        unique_ordered_keys_.clear();
        size_t write_count = 0;
        size_t read_count = 0;
        fiu_mix_workload_keys_.clear();
        while (std::getline(infile, line)) {
            std::istringstream iss(line);
            if (IsWhitespaceOnly(line)) {
                continue;
            }
            std::string token;
            std::getline(iss, token, ',');  // Extract the key
            KEY_TYPE key = std::stoull(token);
            unique_ordered_keys_.push_back(key);
            read_keys_.push_back(key);

            std::getline(iss, token, ',');  // Extract the operation
            std::string operation = token;
            if (operation == "W") {  // Write-'W'
                fiu_mix_workload_keys_.push_back(std::make_pair(key, kInsert));
                write_count++;
            } else if (operation == "R") {  // Read-'R'
                fiu_mix_workload_keys_.push_back(std::make_pair(key, kRead));
                read_count++;
            } else {
                std::cerr << "[ReadFiuTrace] Warning: The operation is neight "
                             "'W' nor 'R'! Check it!"
                          << std::endl;
                return false;
            }
        }
        count_ = unique_ordered_keys_.size();
        infile.close();
        std::cout << "[ReadFiuTrace] Total count: " << read_count + write_count
                  << "; read count: " << read_count << ", read pct.: "
                  << read_count * 100 / (read_count + write_count)
                  << " %; write count: " << write_count << ", write pct.: "
                  << write_count * 100 / (read_count + write_count) << " %"
                  << std::endl;
        return true;
    }

    bool FromFile(const std::string& filename) {
        if (filename.find("fiu") != std::string::npos) {
            std::cout << "[FromFile] Deal with FIU traces: " << filename
                      << std::endl;
            return ReadFiuTrace(filename);
        } else if (filename.find("umass") != std::string::npos ||
                   filename.find(".csv") != std::string::npos) {
            std::cout << "[FromFile] Deal with text file: " << filename
                      << std::endl;
            return FromTextFile(filename);
        }
        std::ifstream infile(filename, std::ios::in | std::ios_base::binary);
        if (!infile.is_open()) {
            std::cout << "[FromFile] Error opening " << filename << std::endl;
            return false;
        }
        uint32_t key_u32;
        size_t key;
        unique_ordered_keys_.clear();
        while (!infile.read(reinterpret_cast<char*>(&key_u32), 4).eof()) {
            key = key_u32;
            unique_ordered_keys_.push_back(key);
            read_keys_.push_back(key);
        }
        count_ = unique_ordered_keys_.size();
        infile.close();
        return true;
    }

    // unique_ordered_keys_ unique
    // read_keys_ full trace
    void FormatKeys(double bulkload_ratio) {
        std::cout << "[FormatKeys] Ensure there is no duplicate keys"
                  << std::endl;

        std::unordered_set<KEY_TYPE>
            unique_unordered_set;  // record the original order of the unique
                                   // keys
        for (size_t i = 0; i < unique_ordered_keys_.size(); ++i) {
            unique_unordered_set.insert(unique_ordered_keys_[i]);
        }

        unique_unsorted_keys_.clear();
        for (const auto& item : unique_unordered_set) {
            unique_unsorted_keys_.push_back(item);
        }
        std::random_shuffle(unique_unsorted_keys_.begin(),
                            unique_unsorted_keys_.end());
        size_t init_keys_size = bulkload_ratio * unique_unsorted_keys_.size();
        bulkload_keys_.clear();
        for (size_t i = 0; i < init_keys_size; ++i) {
            bulkload_keys_.push_back(unique_unsorted_keys_[i]);
        }
        std::cout << "[FormatKeys] bulkload ratio=" << std::fixed
                  << std::setprecision(2) << bulkload_ratio * 100
                  << "%, bulkloading keys=" << bulkload_keys_.size()
                  << std::endl;
        std::sort(bulkload_keys_.begin(), bulkload_keys_.end());

        std::sort(unique_ordered_keys_.begin(), unique_ordered_keys_.end());
        size_t prev_num_keys = unique_ordered_keys_.size();
        auto it = std::unique(unique_ordered_keys_.begin(),
                              unique_ordered_keys_.end());
        unique_ordered_keys_.erase(it, unique_ordered_keys_.end());
        count_ = unique_ordered_keys_.size();

        std::cout << "[FormatKeys] Num of keys before format: " << prev_num_keys
                  << ", Num of keys after format: "
                  << unique_ordered_keys_.size() << ", avg repeat times: "
                  << prev_num_keys / (unique_ordered_keys_.size())
                  << ", first key: " << unique_ordered_keys_.front()
                  << ", last key: " << unique_ordered_keys_.back()
                  << ", num of bulkloading keys: " << init_keys_size
                  << ", unqiue_unsorted_keys_.size= "
                  << unique_unsorted_keys_.size() << std::endl;
    }

    ~RandomKeyTrace() {}

    // read / write ratio
    void GenerateMixWorkload(uint64_t& operation_num, int read_ratio,
                             int insert_ratio, int delete_ratio,
                             int update_ratio, int scan_ratio) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dis(0, 99);
        uint64_t read_counter = 0;
        uint64_t insert_counter = bulkload_keys_.size();
        uint64_t delete_counter = 0.5 * unique_unsorted_keys_.size();
        mix_workload_keys_.clear();

        // statistics info for debug
        size_t stats_op_sum = 0;  // real total op count
        size_t stats_read = 0;    // real read count
        size_t stats_insert = 0;  // real insert count
        size_t stats_update = 0;  // real update count
        size_t stats_delete = 0;  // real delete count
        size_t stats_scan = 0;    // real scan count
        std::unordered_set<KEY_TYPE> insert_keys;
        for (size_t i = 0; i < bulkload_keys_.size(); ++i) {
            insert_keys.insert(bulkload_keys_[i]);
        }
        for (uint64_t i = 0; i < read_keys_.size(); i++) {
            int prob = dis(gen);
            if (prob < read_ratio) {  // read
                mix_workload_keys_.push_back(
                    std::make_pair(read_keys_[i], kRead));
                stats_read++;
            } else if (prob < read_ratio + insert_ratio) {  // write
                if (insert_keys.find(read_keys_[i]) == insert_keys.end()) {
                    mix_workload_keys_.push_back(
                        std::make_pair(read_keys_[i], kInsert));
                    insert_keys.insert(read_keys_[i]);
                } else {
                    mix_workload_keys_.push_back(
                        std::make_pair(read_keys_[i], kUpdate));
                }
                stats_insert++;
            } else if (prob <
                       read_ratio + insert_ratio + update_ratio) {  // update
                mix_workload_keys_.push_back(
                    std::make_pair(read_keys_[i], kUpdate));
                stats_update++;
            } else if (prob <
                       read_ratio + insert_ratio + update_ratio + scan_ratio) {
                mix_workload_keys_.push_back(
                    std::make_pair(read_keys_[i], kScan));
                stats_scan++;
            } else {
                std::cout << "debug: prob=" << prob << std::endl;
                if (delete_counter >= unique_unsorted_keys_.size()) {
                    operation_num = i;
                    break;
                }
                mix_workload_keys_.push_back(std::make_pair(
                    unique_unsorted_keys_[delete_counter++], kDelete));
                stats_delete++;
            }
        }
        std::cout << "[MixWorkload] bulkload_keys_.size="
                  << bulkload_keys_.size()
                  << ", insert_keys.size=" << insert_keys.size()
                  << "; (updated keys:" << stats_insert - insert_keys.size()
                  << ")" << std::endl;
        stats_op_sum = stats_read + stats_insert + stats_update + stats_scan +
                       stats_delete;
        std::cout << "[MixWorkload] mix_workload_keys_.size="
                  << mix_workload_keys_.size()
                  << ", stats_op_sum: " << stats_op_sum
                  << ", real ratio: " << std::endl;
        std::cout << "read: " << stats_read
                  << "; pct:" << (double)(stats_read) * 100 / stats_op_sum
                  << " %" << std::endl;
        std::cout << "insert: " << stats_insert
                  << "; pct:" << (double)(stats_insert) * 100 / stats_op_sum
                  << " %" << std::endl;
        std::cout << "update: " << stats_update
                  << "; pct:" << (double)(stats_update) * 100 / stats_op_sum
                  << " %" << std::endl;
        std::cout << "scan: " << stats_scan
                  << "; pct:" << (double)(stats_scan) * 100 / stats_op_sum
                  << " %" << std::endl;
        std::cout << "delete: " << stats_delete
                  << "; pct:" << (double)(stats_delete) * 100 / stats_op_sum
                  << " %" << std::endl;

        // exit (0);
    }

    void GenerateInsertWorkload(double percentage) {
        std::unordered_set<uint64_t> mySet;
        // Before execute this function, we have read the data from the file
        // 1. Check how many unique number
        for (uint64_t i = 0; i < unique_ordered_keys_.size(); i++) {
            mySet.insert(unique_ordered_keys_[i]);
        }
        uint64_t threshold = percentage * mySet.size();
        std::cout << "unique numbers in SET: " << mySet.size()
                  << "; unique_ordered_keys_.size="
                  << unique_ordered_keys_.size() << "; threshold=" << threshold
                  << std::endl;

        uint64_t indexToDeleteFrom = 0;
        bool reach_threshold = 0;
        mySet.clear();
        // 2. set the flag for each operation
        for (uint64_t i = 0; i < unique_ordered_keys_.size(); i++) {
            if (reach_threshold) {
                // reaches the threshold, check if the key exists in the SET
                if (mySet.find(unique_ordered_keys_[i]) != mySet.end()) {
                    // found the key in the SET, it means current ops should be
                    // read
                    keys_type_ops_.push_back(
                        std::make_pair(unique_ordered_keys_[i], 0));
                } else {
                    keys_type_ops_.push_back(std::make_pair(
                        unique_ordered_keys_[i], 1));  // 1 Flag means write
                }
            } else {
                keys_type_ops_.push_back(std::make_pair(
                    unique_ordered_keys_[i], 0));  // 0 Flag means read
            }

            mySet.insert(unique_ordered_keys_[i]);
            if (!reach_threshold && mySet.size() >= threshold) {
                indexToDeleteFrom = i;
                reach_threshold = 1;
            }
        }

        // 3. delete the keys after threshold
        unique_ordered_keys_.erase(
            unique_ordered_keys_.begin() + indexToDeleteFrom + 1,
            unique_ordered_keys_.end());
        count_ = unique_ordered_keys_.size();
        std::cout << "indexToDelFrom " << indexToDeleteFrom
                  << ", write keys remain " << count_
                  << "; keys_type_ops.size= " << keys_type_ops_.size()
                  << std::endl;

        uint64_t insert = 0;
        uint64_t read = 0;
        size_t index = 0;
        for (auto& item : keys_type_ops_) {
            index++;
            if (item.second == 1) {
                insert++;
                continue;
            }
            read++;
        }
        printf("[GenerateInsertWorkload] read %lu insert %lu\n", read, insert);
    }

    void PrepareDataForExp2(
        const std::unordered_set<KEY_TYPE>& points_on_line) {
        if (points_on_line.empty()) {
            std::cout << "[PrepareDataForExp2] points_on_line is empty"
                      << std::endl;
            return;
        }
        std::cout << "[PrepareDataForExp2] read_keys_.size="
                  << read_keys_.size() << std::endl;

        for (size_t i = 0; i < read_keys_.size(); ++i) {
            KEY_TYPE key = read_keys_[i];
            if (points_on_line.find(key) != points_on_line.end()) {
                line_keys_.push_back(key);
            } else {
                nonline_keys_.push_back(key);
            }
        }
        std::cout << "[PrepareDataForExp2] line_keys_.size="
                  << line_keys_.size()
                  << "; nonline_keys_.size=" << nonline_keys_.size()
                  << std::endl;
    }

    // generate num_keys deleted keys: randomly pick up or not
    void GenDeletedKeys(size_t num_keys, bool is_random, size_t pos = 0) {
        if (num_keys <= 0 || num_keys > unique_ordered_keys_.size() ||
            num_keys + pos > unique_ordered_keys_.size()) {
            std::cerr << "[GenDeletedKeys] Invalid num value=" << num_keys
                      << ", pos=" << pos << ", unique_ordered_keys_.size="
                      << unique_ordered_keys_.size() << std::endl;
            return;
        }
        deleted_keys_.clear();

        // debug
        std::ifstream input("deleted_keys.log", std::ios::in);
        if (input.is_open()) {
            std::string line = "";
            while (std::getline(input, line)) {
                std::istringstream iss(line);
                KEY_TYPE key;
                iss >> key;
                deleted_keys_.push_back(key);
                // deleted_keys_set_.insert (key);
            }

            if (pos == 0) {
                std::sort(deleted_keys_.begin(), deleted_keys_.end());
                std::cout << "[GenDeletedKeys] sorting deleted keys"
                          << std::endl;
            }
            input.close();
            std::cout << "[GenDeletedKeys] deleted keys from file: size="
                      << deleted_keys_.size() << ", num_keys=" << num_keys
                      << ", is_random=" << is_random << ", pos=" << pos
                      << std::endl;
            return;
        }

        std::unordered_set<KEY_TYPE> tmp_set;  // used to remove repeated keys
        if (is_random) {
            if (num_keys == unique_ordered_keys_.size()) {
                deleted_keys_ = unique_ordered_keys_;
                return;
            }
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<KEY_TYPE> dis(
                0, unique_ordered_keys_.size() - 1);

            while (tmp_set.size() < num_keys) {
                int random_index = dis(gen);
                KEY_TYPE key = unique_ordered_keys_[random_index];

                tmp_set.insert(key);
            }

            deleted_keys_.assign(tmp_set.begin(), tmp_set.end());

            // debug
            // test if only unsorted deleted keys will trigger the seg-fault
            if (pos == 0) {
                std::sort(deleted_keys_.begin(), deleted_keys_.end());
            }
            std::ofstream log("deleted_keys.log");
            if (!log.is_open()) {
                std::cout << "[SaveVec2File] Fail to open the file: "
                          << std::endl;
                return;
            }

            for (size_t i = 0; i < deleted_keys_.size(); ++i) {
                log << deleted_keys_[i] << "\n";
            }
            log.close();

        } else {
            for (size_t i = pos; i < pos + num_keys; ++i) {
                KEY_TYPE key = unique_ordered_keys_[i];
                // key |= (KEY_TYPE)1 << (sizeof (KEY_TYPE) - 1);
                deleted_keys_.push_back(key);
            }
        }
        std::cout << "[GenDeletedKeys] deleted keys size="
                  << deleted_keys_.size() << ", num_keys=" << num_keys
                  << ", is_random=" << is_random << ", pos=" << pos
                  << std::endl;
    }

    void Randomize(void) {
        printf("randomize %lu keys\n", unique_ordered_keys_.size());
        auto starttime = std::chrono::system_clock::now();
        std::random_shuffle(unique_ordered_keys_.begin(),
                            unique_ordered_keys_.end());
        std::random_shuffle(read_keys_.begin(), read_keys_.end());
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        printf("randomize duration %f s.\n", duration.count() / 1000000.0);
    }

    class RangeIterator {
       public:
        RangeIterator(std::vector<size_t>* pkey_vec, size_t start, size_t end,
                      size_t size)
            : pkey_vec_(pkey_vec), end_index_(end), cur_index_(start) {
            if (end > size) {
                end_index_ = size;
            }
        }

        RangeIterator(std::vector<std::pair<size_t, bool>>* pkey_vec,
                      size_t start, size_t end, size_t size)
            : pkey_pair_vec_(pkey_vec), end_index_(end), cur_index_(start) {
            if (end > size) {
                end_index_ = size;
            }
        }

        RangeIterator(std::vector<std::pair<size_t, Operation>>* pkey_vec,
                      size_t start, size_t end, size_t size)
            : pkey_op_pair_vec_(pkey_vec), end_index_(end), cur_index_(start) {
            if (end > size) {
                end_index_ = size;
            }
        }

        inline bool Valid() { return (cur_index_ < end_index_); }

        inline size_t operator*() { return (*pkey_vec_)[cur_index_]; }

        inline size_t& Next() { return (*pkey_vec_)[cur_index_++]; }
        inline std::pair<size_t, bool>& NextPair() {
            return (*pkey_pair_vec_)[cur_index_++];
        }
        inline std::pair<size_t, Operation>& NextOpPair() {
            return (*pkey_op_pair_vec_)[cur_index_++];
        }
        inline void RollBack() { cur_index_ = cur_index_ - 6000; }

        std::vector<size_t>* pkey_vec_;
        std::vector<std::pair<size_t, bool>>* pkey_pair_vec_;
        std::vector<std::pair<size_t, Operation>>* pkey_op_pair_vec_;
        size_t end_index_;
        size_t cur_index_;
    };

    class Iterator {
       public:
        Iterator(std::vector<size_t>* pkey_vec, size_t start_index,
                 size_t range)
            : pkey_vec_(pkey_vec),
              range_(range),
              end_index_(start_index % range_),
              cur_index_(start_index % range_),
              begin_(true) {}

        Iterator() {}

        inline bool Valid() { return (begin_ || cur_index_ != end_index_); }

        inline size_t& Next() {
            begin_ = false;
            size_t index = cur_index_;
            cur_index_++;
            if (cur_index_ >= range_) {
                cur_index_ = 0;
            }
            return (*pkey_vec_)[index];
        }

        inline size_t operator*() { return (*pkey_vec_)[cur_index_]; }

        std::string printf() {
            char buffer[128];
            sprintf(buffer, "valid: %s, cur i: %lu, end_i: %lu, range: %lu",
                    Valid() ? "true" : "false", cur_index_, end_index_, range_);
            return buffer;
        }

        std::vector<size_t>* pkey_vec_;
        size_t range_;
        size_t end_index_;
        size_t cur_index_;
        bool begin_;
    };

    Iterator trace_at(size_t start_index, size_t range) {
        return Iterator(&unique_ordered_keys_, start_index, range);
    }

    RangeIterator Begin(void) {
        return RangeIterator(&unique_ordered_keys_, 0,
                             unique_ordered_keys_.size());
    }

    RangeIterator iterate_between(size_t start, size_t end) {
        return RangeIterator(&unique_ordered_keys_, start, end,
                             unique_ordered_keys_.size());
    }

    RangeIterator read_iterate_between(size_t start, size_t end) {
        return RangeIterator(&read_keys_, start, end, read_keys_.size());
    }

    RangeIterator ops_type_iterate_between(size_t start, size_t end) {
        return RangeIterator(&keys_type_ops_, start, end,
                             keys_type_ops_.size());
    }

    RangeIterator mix_workload_iterate_between(size_t start, size_t end) {
        return RangeIterator(&mix_workload_keys_, start, end,
                             mix_workload_keys_.size());
    }

    RangeIterator linekeys_iterate_between(size_t start, size_t end) {
        return RangeIterator(&line_keys_, start, end, line_keys_.size());
    }

    RangeIterator nonlinekeys_iterate_between(size_t start, size_t end) {
        return RangeIterator(&nonline_keys_, start, end, nonline_keys_.size());
    }
    RangeIterator delete_iterate_between(size_t start, size_t end) {
        return RangeIterator(&deleted_keys_, start, end, deleted_keys_.size());
    }

    size_t count_;
    std::vector<KEY_TYPE> bulkload_keys_;        // unique keys, for bulkload
    std::vector<KEY_TYPE> unique_ordered_keys_;  // unique sorted keys, for
                                                 // checking lookup results
    // for read-only experiments, unique_ordered_keys_ equals to bulkload_keys_
    std::vector<KEY_TYPE>
        unique_unsorted_keys_;         // unique unsorted keys, for bulkload
    std::vector<KEY_TYPE> read_keys_;  // original keys: not unique, unordered
    std::vector<KEY_TYPE> deleted_keys_;
    // std::unordered_set<KEY_TYPE> deleted_keys_set_;  // debug
    std::vector<std::pair<KEY_TYPE, bool>> keys_type_ops_;
    std::vector<std::pair<KEY_TYPE, Operation>>
        mix_workload_keys_;  // keys with mix workload flag
    std::vector<std::pair<KEY_TYPE, Operation>>
        fiu_mix_workload_keys_;  // keys with mix workload flag for FIU traces

    // for insert experiments
    std::vector<KEY_TYPE> line_keys_;     // keys that on lines
    std::vector<KEY_TYPE> nonline_keys_;  // keys that not on lines
};

enum YCSBOpType { kYCSB_Write, kYCSB_Read, kYCSB_Query, kYCSB_ReadModifyWrite };

inline uint32_t wyhash32() {
    static thread_local uint32_t wyhash32_x = random();
    wyhash32_x += 0x60bee2bee120fc15;
    uint64_t tmp;
    tmp = (uint64_t)wyhash32_x * 0xa3b195354a39b70d;
    uint32_t m1 = (tmp >> 32) ^ tmp;
    tmp = (uint64_t)m1 * 0x1b03738712fad5c9;
    uint32_t m2 = (tmp >> 32) ^ tmp;
    return m2;
}

class YCSBGenerator {
   public:
    // Generate
    YCSBGenerator() {}

    inline YCSBOpType NextA() {
        // ycsba: 50% reads, 50% writes
        uint32_t rnd_num = wyhash32();

        if ((rnd_num & 0x1) == 0) {
            return kYCSB_Read;
        } else {
            return kYCSB_Write;
        }
    }

    inline YCSBOpType NextB() {
        // ycsbb: 95% reads, 5% writes
        // 51/1024 = 0.0498
        uint32_t rnd_num = wyhash32();

        if ((rnd_num & 1023) < 51) {
            return kYCSB_Write;
        } else {
            return kYCSB_Read;
        }
    }

    inline YCSBOpType NextC() { return kYCSB_Read; }

    inline YCSBOpType NextD() {
        // ycsbd: read latest inserted records
        uint32_t rnd_num = wyhash32();
        if ((rnd_num & 1023) < 51) {
            return kYCSB_Write;
        } else {
            return kYCSB_Read;
        }
    }

    inline YCSBOpType NextE() {
        // ycsbd: read latest inserted records
        uint32_t rnd_num = wyhash32();
        if ((rnd_num & 1023) < 51) {
            return kYCSB_Write;
        } else {
            return kYCSB_Read;
        }
    }

    inline YCSBOpType NextF() {
        // ycsba: 50% reads, 50% writes
        uint32_t rnd_num = wyhash32();

        if ((rnd_num & 0x1) == 0) {
            return kYCSB_Read;
        } else {
            return kYCSB_ReadModifyWrite;
        }
    }
};
