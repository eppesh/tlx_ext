#include <cxxabi.h>
#include <execinfo.h>
#include <gflags/gflags.h>
#include <immintrin.h>
#include <unistd.h>

#include <condition_variable>  // std::condition_variable
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>  // std::mutex
#include <stack>
#include <thread>  // std::thread
#include <tlx/container/btree_map.hpp>
#include <tlx/container/btree_set.hpp>

#include "histogram_me.h"
#include "random_generator.h"
// #include "tbb/parallel_sort.h"
#include "test_util_me.h"

DEFINE_bool(verbose, false, "Enable verbose output");
DEFINE_string(name, "World", "Name to greet");

/* using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;
 */
using namespace util;

DEFINE_uint32(batch, 100, "report batch");
DEFINE_uint32(readtime, 0, "if 0, then we read all keys");
DEFINE_uint32(worker_threads, 1, "# of threads");
DEFINE_uint64(report_interval, 1, "Report interval in seconds");
DEFINE_uint64(stats_interval, 100000000, "Report interval in ops");
DEFINE_uint64(value_size, 8, "The value size");
DEFINE_uint64(num, 2 * 1000000LU, "Number of total record");
DEFINE_uint64(read, 0, "Number of read operations");
DEFINE_uint64(write, 0, "Number of read operations");
DEFINE_bool(hist, false, "");
DEFINE_string(benchmarks, "load,readall", "");
DEFINE_string(tracefile, "randomtrace.data", "");
DEFINE_bool(is_seq, false, "enable the sequential trace");
DEFINE_bool(use_interval, false, "whether to use interval in multi-thread");

DEFINE_double(bulk_load_ratio, 0.5, "the ratio of keys for bulk loading");
DEFINE_uint64(read_ratio, 100, "the ratio of read operation");
DEFINE_uint64(insert_ratio, 0, "the ratio of insert operation");
DEFINE_uint64(delete_ratio, 0, "the ratio of delete operation");
DEFINE_uint64(update_ratio, 0, "the ratio of update operation");
DEFINE_uint64(scan_ratio, 0, "the ratio of scan operation");
DEFINE_uint64(operation_num, 800000000, "the num of operations");

///////////////////////////////
const int STACK_START_TO_PRINT = 3;
const int NUM_STACK_TO_PRINT = 4;

template <typename KeyType>
struct traits_nodebug : tlx::btree_default_traits<KeyType, KeyType> {
    static const bool self_verify = false;
    static const bool debug = false;

    static const int leaf_slots = 8;
    static const int inner_slots = 8;
};

std::string format_time(std::chrono::time_point<std::chrono::system_clock> ts) {
    // Get the current time from system_clock
    auto now = ts;

    // Convert to time_t to get calendar time
    std::time_t time_t_now = std::chrono::system_clock::to_time_t(now);

    // Convert time_t to tm for formatting
    std::tm tm_now = *std::localtime(&time_t_now);

    // Get the duration since the epoch
    auto duration_since_epoch = now.time_since_epoch();

    // Extract seconds and nanoseconds from the duration
    auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(duration_since_epoch);
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                            duration_since_epoch) -
                        seconds;

    // Format the output string
    std::stringstream ss;
    ss << std::put_time(&tm_now, "%Y-%m-%d %H:%M:%S");
    ss << '.' << std::setw(6) << std::setfill('0') << microseconds.count();

    return ss.str();
}

std::string format_current_time() {
    // Get the current time from system_clock
    auto now = std::chrono::system_clock::now();
    return format_time(now);
}

void split_sym(char* input, std::vector<std::string>* resultp) {
    std::istringstream iss(input);
    std::string word;

    auto& result = *resultp;
    result.clear();

    // Use a loop to split the string by spaces
    while (iss >> word) {
        result.push_back(word);
    }
}

std::string remove_text_between_brackets(const std::string& input) {
    std::string result;
    std::stack<char> bracket_stack;
    bool remove_text = false;

    for (char ch : input) {
        if (ch == '(' || ch == '[' || ch == '{' || ch == '<') {
            bracket_stack.push(ch);
            remove_text = true;
        } else if ((ch == ')' && !bracket_stack.empty() &&
                    bracket_stack.top() == '(') ||
                   (ch == ']' && !bracket_stack.empty() &&
                    bracket_stack.top() == '[') ||
                   (ch == '}' && !bracket_stack.empty() &&
                    bracket_stack.top() == '{') ||
                   (ch == '>' && !bracket_stack.empty() &&
                    bracket_stack.top() == '<')) {
            bracket_stack.pop();
            if (bracket_stack.empty()) {
                remove_text = false;
            }
        } else if (!remove_text) {
            result += ch;
        }
    }

    return result;
}

// Function to extract the last word from a string
std::string extract_func_name(const std::string& input) {
    auto clean_input = remove_text_between_brackets(input);
    ssize_t pos = clean_input.find_last_not_of(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_");
    auto res = clean_input.substr(pos + 1);
    return res;
}

std::string stack_sym(void* const addrs[NUM_STACK_TO_PRINT]) {
    char** strs = backtrace_symbols(addrs, NUM_STACK_TO_PRINT);
    int status;
    std::vector<std::string> names;
    std::string s = "";

    for (int i = 0; i < NUM_STACK_TO_PRINT; ++i) {
        split_sym(strs[i], &names);
        char* demangled_name =
            abi::__cxa_demangle(names[3].c_str(), 0, 0, &status);
        if (demangled_name == nullptr) {
            s += "(null) ";
        } else {
            std::string func_name = extract_func_name(demangled_name);
            std::stringstream stream;
            stream << func_name << '(' << std::hex << addrs[i] << ") ";
            s += stream.str();
            free(demangled_name);
        }
    }
    return s;
}
typedef tlx::btree_set<unsigned int, std::less<unsigned int>,
                       traits_nodebug<unsigned int> >
    set_type;

const int MAX_KEY = 100;
const int NUM_OPERATIONS = 100;

struct Entry {
    std::mutex mtx;
    bool in_set = false;
};

std::vector<Entry> truth_source(MAX_KEY);

std::mutex printmtx;
int seqnum = 0;
set_type my_multi_thread_set;

const int NUM_THREADS = 16;
int cur_numthreads = 1;
const int thread_start_idx = 2;
const bool debug_print = false;

// Global array of thread information
std::vector<thread_info> global_thread_info(NUM_THREADS);
std::atomic<int> thread_count(0);
std::map<std::thread::id, int> thread_id_map;

enum LogType {
    LOG_LOCK,
    LOG_MEM_OP,
    LOG_RETRY,
};

struct LogInfo {
    LogType logtype;
    std::chrono::time_point<std::chrono::system_clock> timestamp;
    void* addrs[NUM_STACK_TO_PRINT];
    int threadidx;
    void* node;

    union {
        struct {  // LOG_LOCK
            int gen;
            unsigned short level;
            unsigned short slotuse;
            unsigned int numreader;
            bool haswriter;
            int writerswaiting;
            int readerswaiting;
            int upgradewaiting;
            int lock_type;
        };
        struct {  // LOG_MEM_OP
            MemOpType mem_op_type;
            int num_inner;
            int num_leaves;
        };
    };
};

enum { TOTAL_DEBUG_LOG_INFO = 10000 };
std::vector<LogInfo> debug_log_info(TOTAL_DEBUG_LOG_INFO);
std::atomic<size_t> cur_debug_log_info = 0;

void get_stack_addr(void* out_addrs[NUM_STACK_TO_PRINT]) {
    const int TOTAL_STACK = STACK_START_TO_PRINT + NUM_STACK_TO_PRINT;
    void* addrs[TOTAL_STACK];

    backtrace(addrs, TOTAL_STACK);
    for (int i = STACK_START_TO_PRINT; i < TOTAL_STACK; ++i) {
        out_addrs[i - STACK_START_TO_PRINT] = addrs[i];
    }
}

void log_retry() {
    if (debug_log_info.empty()) return;

    size_t idx = cur_debug_log_info.fetch_add(1, std::memory_order_relaxed);

    LogInfo& log_info = debug_log_info[idx % TOTAL_DEBUG_LOG_INFO];
    log_info.logtype = LOG_RETRY;
    get_stack_addr(log_info.addrs);
}

void log_mem_op(MemOpType optype, void* node, int num_inner, int num_leaves) {
    if (debug_log_info.empty()) return;

    size_t idx = cur_debug_log_info.fetch_add(1, std::memory_order_relaxed);

    LogInfo& log_info = debug_log_info[idx % TOTAL_DEBUG_LOG_INFO];
    log_info.logtype = LOG_MEM_OP;
    log_info.node = node;
    log_info.mem_op_type = optype;
    log_info.timestamp = std::chrono::system_clock::now();
    log_info.threadidx =
        local_debug_info.tinfo ? local_debug_info.tinfo->threadidx : 0;
    log_info.num_inner = num_inner;
    log_info.num_leaves = num_leaves;

    get_stack_addr(log_info.addrs);
}

void log_lock(void* node, int lock_type) {
    if (cur_numthreads > 1 && local_debug_info.tinfo) {
        local_debug_info.tinfo->cur_node = node;
        local_debug_info.tinfo->op = lock_type;

        size_t idx = cur_debug_log_info.fetch_add(1, std::memory_order_relaxed);

        LogInfo& log_info = debug_log_info[idx % TOTAL_DEBUG_LOG_INFO];
        log_info.logtype = LOG_LOCK;
        log_info.timestamp = std::chrono::system_clock::now();
        log_info.threadidx = local_debug_info.tinfo->threadidx;
        log_info.node = node;

        set_type::btree_impl::node* nodep =
            static_cast<set_type::btree_impl::node*>(node);
        log_info.gen = nodep->gen;
        log_info.level = nodep->level;
        log_info.slotuse = nodep->slotuse;
        log_info.numreader = nodep->lock->numreader;
        log_info.haswriter = nodep->lock->haswriter;
        log_info.readerswaiting = nodep->lock->readerswaiting;
        log_info.writerswaiting = nodep->lock->writerswaiting;
        log_info.upgradewaiting = nodep->lock->upgradewaiting;
        log_info.lock_type = lock_type;

        get_stack_addr(log_info.addrs);
    }
}

const char* MemOpName[] = {"alloc inner", "alloc leaf", "free inner",
                           "free leaf"};

void print_lock_record(const LogInfo& info) {
    std::lock_guard<std::mutex> printlock(printmtx);
    switch (info.logtype) {
        case LOG_LOCK:
            if (info.node == 0) return;

            std::cout << format_time(info.timestamp) << " thread "
                      << info.threadidx << " node " << info.node << " (g"
                      << info.gen << " c" << info.slotuse << " L" << info.level
                      << ") (r" << info.numreader << "|w" << info.haswriter
                      << " waiter:r" << info.readerswaiting << "|w"
                      << info.writerswaiting << "|u" << info.upgradewaiting
                      << ") " << lock_type_to_string(info.lock_type) << " "
                      << stack_sym(info.addrs) << std::endl;
            break;
        case LOG_MEM_OP:
            std::cout << format_time(info.timestamp) << " "
                      << MemOpName[info.mem_op_type] << " " << info.node
                      << " #inner=" << info.num_inner
                      << " #leaves=" << info.num_leaves << " "
                      << stack_sym(info.addrs) << std::endl;
            break;
        case LOG_RETRY:
            std::cout << format_time(info.timestamp) << " retry "
                      << stack_sym(info.addrs) << std::endl;
            break;
    }
}

void print_all_lock_records() {
    size_t i;
    size_t cur_index = cur_debug_log_info % TOTAL_DEBUG_LOG_INFO;
    for (i = cur_index; i < debug_log_info.size(); ++i) {
        print_lock_record(debug_log_info[i]);
    }

    for (i = 0; i < cur_index; ++i) {
        print_lock_record(debug_log_info[i]);
    }
}

void print_threads_states(void) {
    my_multi_thread_set.print(std::cout);
    for (int i = 0; i < cur_numthreads; ++i) {
        std::cout << "Thread " << i + thread_start_idx
                  << " id: " << global_thread_info[i].id
                  << " - Node: " << global_thread_info[i].cur_node
                  << ", Operation: "
                  << lock_type_to_string(global_thread_info[i].op) << std::endl;
        if (global_thread_info[i].cur_node) {
            set_type::btree_impl::node* nodep =
                static_cast<set_type::btree_impl::node*>(
                    global_thread_info[i].cur_node);
            auto lock = nodep->lock;
            if (lock == nullptr) {
                std::cout << "  lock=null\n";
                continue;
            }
            std::cout << "  curread: ";
            std::set<int> ids;  // print all ids in order
            for (auto id : lock->curread) {
                ids.insert(thread_id_map[id]);
            }
            for (auto id : ids) {
                std::cout << id << ' ';
            }
            ids.clear();
            std::cout << std::endl;
            std::cout << "  curwrite: ";
            for (auto id : lock->curwrite) {
                ids.insert(thread_id_map[id]);
            }
            for (auto id : ids) {
                std::cout << id << ' ';
            }
            std::cout << std::endl;
        }
    }
}
// Signal handler for SIGUSR1
void signal_handler(int signum) {
    if (signum == SIGUSR1) {
        std::cout << "Received SIGUSR1. Current thread states:\n";
        print_all_lock_records();
        print_threads_states();
    } else {
        std::cout << "Received signal " << signum << std::endl;
    }
}

// Function to initialize thread debug info
void initialize_thread_info(int index) {
    std::lock_guard<std::mutex> lock(printmtx);
    local_debug_info.tinfo = &global_thread_info[index];
    local_debug_info.tinfo->id = std::this_thread::get_id();
    local_debug_info.tinfo->threadidx = index + thread_start_idx;
    local_debug_info.tinfo->cur_node = nullptr;
    local_debug_info.tinfo->op = 0;
    thread_id_map[std::this_thread::get_id()] = index + thread_start_idx;
}

// Function to cleanup thread debug info
void cleanup_thread_info() {
    local_debug_info.tinfo->cur_node = nullptr;
    local_debug_info.tinfo->op = 0;
}

void before_assert(void) {
    static bool tree_printed = false;
    if (!tree_printed) {  // only print once
        tree_printed = true;
        print_all_lock_records();
        std::lock_guard<std::mutex> l(printmtx);
        std::cout << "======= print thread state before assert =======\n";
        print_threads_states();
        std::cout << std::endl;
        return;
    } else {
        sleep(3600);
    }
}

//////////////////////////////////

namespace {
class Stats {
   public:
    int tid_;
    double start_;
    double finish_;
    double seconds_;
    double next_report_time_;
    double last_op_finish_;
    unsigned last_level_compaction_num_;
    Histogram hist_;

    uint64_t done_;
    uint64_t last_report_done_;
    uint64_t last_report_finish_;
    uint64_t next_report_;
    std::string message_;

    Stats() { Start(); }
    explicit Stats(int id) : tid_(id) { Start(); }

    void Start() {
        start_ = NowMicros();
        next_report_time_ = start_ + FLAGS_report_interval * 1000000;
        next_report_ = 100;
        last_op_finish_ = start_;
        last_report_done_ = 0;
        last_report_finish_ = start_;
        last_level_compaction_num_ = 0;
        done_ = 0;
        seconds_ = 0;
        finish_ = start_;
        message_.clear();
        hist_.Clear();
    }

    void Merge(const Stats& other) {
        hist_.Merge(other.hist_);
        done_ += other.done_;
        seconds_ += other.seconds_;
        if (other.start_ < start_) start_ = other.start_;
        if (other.finish_ > finish_) finish_ = other.finish_;

        // Just keep the messages from one thread
        if (message_.empty()) message_ = other.message_;
    }

    void Stop() {
        finish_ = NowMicros();
        seconds_ = (finish_ - start_) * 1e-6;
        ;
    }

    void StartSingleOp() { last_op_finish_ = NowMicros(); }

    void PrintSpeed() {
        uint64_t now = NowMicros();
        // int64_t usecs_since_last = now - last_report_finish_;

        std::string cur_time = TimeToString(now / 1000000);
        // printf (
        //     "%s ... thread %d: (%lu,%lu) ops and "
        //     "( %.1f,%.1f ) ops/second in (%.4f,%.4f) seconds\n",
        //     cur_time.c_str (), tid_, done_ - last_report_done_, done_,
        //     (done_ - last_report_done_) / (usecs_since_last / 1000000.0),
        //     done_ / ((now - start_) / 1000000.0), (now -
        //     last_report_finish_) / 1000000.0, (now - start_) /
        //     1000000.0);
        printf("[Epoch] %d,%lu,%lu,%.4f,%.4f\n", tid_,
               done_ - last_report_done_, done_,
               (now - last_report_finish_) / 1000000.0,
               (now - start_) / 1000000.0);

        last_report_finish_ = now;
        last_report_done_ = done_;
        fflush(stdout);
    }

    static void AppendWithSpace(std::string* str, const std::string& msg) {
        if (msg.empty()) return;
        if (!str->empty()) {
            str->push_back(' ');
        }
        str->append(msg.data(), msg.size());
    }

    void AddMessage(const std::string& msg) { AppendWithSpace(&message_, msg); }

    inline bool FinishedBatchOp(size_t batch) {
        double now = NowNanos();
        last_op_finish_ = now;
        done_ += batch;
        if ((done_ >= next_report_)) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf(stderr, "... finished %llu ops%30s\r",
                    (unsigned long long)done_, "");

            if (FLAGS_report_interval == 0 &&
                (done_ % FLAGS_stats_interval) == 0) {
                PrintSpeed();
                return 0;
            }
            fflush(stderr);
            fflush(stdout);
        }

        if (FLAGS_report_interval != 0 && NowMicros() > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            PrintSpeed();
            return 1;
        }
        return 0;
    }

    inline void FinishedSingleOp() {
        double now = NowNanos();
        last_op_finish_ = now;

        done_++;
        if (done_ >= next_report_) {
            if (next_report_ < 1000)
                next_report_ += 100;
            else if (next_report_ < 5000)
                next_report_ += 500;
            else if (next_report_ < 10000)
                next_report_ += 1000;
            else if (next_report_ < 50000)
                next_report_ += 5000;
            else if (next_report_ < 100000)
                next_report_ += 10000;
            else if (next_report_ < 500000)
                next_report_ += 50000;
            else
                next_report_ += 100000;
            fprintf(stderr, "... finished %llu ops%30s\r",
                    (unsigned long long)done_, "");

            if (FLAGS_report_interval == 0 &&
                (done_ % FLAGS_stats_interval) == 0) {
                PrintSpeed();
                return;
            }
            fflush(stderr);
            fflush(stdout);
        }

        if (FLAGS_report_interval != 0 && NowMicros() > next_report_time_) {
            next_report_time_ += FLAGS_report_interval * 1000000;
            PrintSpeed();
        }
    }

    std::string TimeToString(uint64_t secondsSince1970) {
        const time_t seconds = (time_t)secondsSince1970;
        struct tm t;
        int maxsize = 64;
        std::string dummy;
        dummy.reserve(maxsize);
        dummy.resize(maxsize);
        char* p = &dummy[0];
        localtime_r(&seconds, &t);
        snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ", t.tm_year + 1900,
                 t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec);
        return dummy;
    }

    void Report(const std::string& name, bool print_hist = false) {
        // Pretend at least one op was done in case we are running a
        // benchmark that does not call FinishedSingleOp().
        if (done_ < 1) done_ = 1;

        std::string extra;

        AppendWithSpace(&extra, message_);

        double elapsed = (finish_ - start_) * 1e-6;

        double throughput = (double)done_ / elapsed;

        printf(
            "%-12s : %11.3f micros/op %lf Mops/s; elapsed:%lf s; "
            "done:%lu;%s%s\n",
            name.c_str(), elapsed * 1e6 / done_, throughput / 1024 / 1024,
            elapsed, done_, (extra.empty() ? "" : " "), extra.c_str());
        if (print_hist) {
            fprintf(stdout, "Nanoseconds per op:\n%s\n",
                    hist_.ToString().c_str());
        }

        if (name.find("readall") != std::string::npos) {
            std::string trace = FLAGS_tracefile;
            std::filesystem::path path(trace);
            std::string file_name = path.stem().string();
            size_t thread_num = FLAGS_worker_threads;
            /* if (name.compare ("readallrs") == 0) {
                std::cout << "[Throughput] " << name << "," << file_name <<
            ","
            << 32 << ","
                          << (throughput / 1024 / 1024) << std::endl;
            } else if (name.compare ("readalllisa") == 0) {
                std::cout << "[Throughput] " << name << "," << file_name <<
            ","
            << FLAGS_lisa_alpha
                          << "," << FLAGS_lisa_epsilon << "," << (throughput
            / 1024 / 1024)
                          << std::endl;
            } else  */
            {
                std::cout << "[ReadThroughput] " << name << "," << thread_num
                          << "," << file_name << ","
                          << (throughput / 1024 / 1024) << std::endl;
            }
        } else if (name.find("update") != std::string::npos) {
            std::string trace = FLAGS_tracefile;
            std::filesystem::path path(trace);
            std::string file_name = path.stem().string();
            size_t thread_num = FLAGS_worker_threads;
            std::cout << "[WriteThroughput] " << name << "," << thread_num
                      << "," << file_name << "," << (throughput / 1024 / 1024)
                      << std::endl;
        } else if (name.find("read_write") != std::string::npos) {
            std::string trace = FLAGS_tracefile;
            std::filesystem::path path(trace);
            std::string file_name = path.stem().string();
            size_t thread_num = FLAGS_worker_threads;
            size_t read_ratio = FLAGS_read_ratio;
            size_t insert_ratio = FLAGS_insert_ratio;
            std::cout << "[MixThroughput] " << name << "," << thread_num << ","
                      << read_ratio << "," << insert_ratio << "," << file_name
                      << "," << (throughput / 1024 / 1024) << std::endl;
        }

        fflush(stdout);
        fflush(stderr);
    }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
    std::mutex mu;
    std::condition_variable cv;
    int total;

    // Each thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done

    int num_initialized;
    int num_done;
    bool start;

    SharedState(int total)
        : total(total), num_initialized(0), num_done(0), start(false) {}
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
    int tid;  // 0..n-1 when running in n threads
    // Random rand;         // Has different seeds for different threads
    Stats stats;
    SharedState* shared;
    YCSBGenerator ycsb_gen;
    ThreadState(int index) : tid(index), stats(index) {}
};

class Duration {
   public:
    Duration(uint64_t max_seconds, int64_t max_ops, int64_t ops_per_stage = 0) {
        max_seconds_ = max_seconds;
        max_ops_ = max_ops;
        ops_per_stage_ = (ops_per_stage > 0) ? ops_per_stage : max_ops;
        ops_ = 0;
        start_at_ = NowMicros();
    }

    inline int64_t GetStage() {
        return std::min(ops_, max_ops_ - 1) / ops_per_stage_;
    }

    inline bool Done(int64_t increment) {
        if (increment <= 0) increment = 1;  // avoid Done(0) and infinite loops

        int64_t prev_ops = ops_;
        ops_ += increment;

        if (max_seconds_) {
            // Recheck every appx 1000 ops (exact iff increment is factor of
            // 1000)
            auto granularity = 1000;
            if ((ops_ / granularity) != ((ops_ - increment) / granularity)) {
                uint64_t now = NowMicros();
                return ((now - start_at_) / 1000000) >= max_seconds_;
            } else {
                return false;
            }
        } else {
            if (prev_ops > max_ops_) {
                return true;
            } else if (ops_ > max_ops_) {
                return false;
            }
            return ops_ > max_ops_;
        }
    }

    inline int64_t Ops() { return ops_; }

   private:
    uint64_t max_seconds_;
    int64_t max_ops_;
    int64_t ops_per_stage_;
    int64_t ops_;
    uint64_t start_at_;
};

#if defined(__linux)
static std::string TrimSpace(std::string s) {
    size_t start = 0;
    while (start < s.size() && isspace(s[start])) {
        start++;
    }
    size_t limit = s.size();
    while (limit > start && isspace(s[limit - 1])) {
        limit--;
    }
    return std::string(s.data() + start, limit - start);
}
#endif

}  // namespace

template <typename KEY_TYPE, typename PAYLOAD_TYPE>
class Benchmark {
    // typedef indexInterface<KEY_TYPE, PAYLOAD_TYPE> index_t;

   public:
    uint64_t num_;
    size_t reads_;
    RandomKeyTrace<KEY_TYPE, PAYLOAD_TYPE>* key_trace_;
    size_t trace_size_;
    size_t initial_capacity_;
    std::string file_name_;

    tlx::btree_map<KEY_TYPE, PAYLOAD_TYPE> tlx_btree_map_;

    Benchmark() : num_(FLAGS_num), reads_(FLAGS_read), key_trace_(nullptr) {
        std::string trace = FLAGS_tracefile;
        std::filesystem::path path(trace);
        file_name_ = path.stem().string();
    }
    ~Benchmark() { delete key_trace_; }
    bool CheckParameters() {
        size_t ratio_sum = FLAGS_read_ratio + FLAGS_insert_ratio +
                           FLAGS_delete_ratio + FLAGS_update_ratio +
                           FLAGS_scan_ratio;
        if (ratio_sum != 100) {
            std::cerr << "[CheckParameters] the sum of ratios (" << ratio_sum
                      << ") should be 100" << std::endl;
            return false;
        }
        if (FLAGS_insert_ratio * FLAGS_delete_ratio != 0) {
            std::cerr << "[CheckParameters] insert_ratio(" << FLAGS_insert_ratio
                      << " %) and delete_ratio(" << FLAGS_delete_ratio
                      << " %) should not appear at the same time." << std::endl;
            return false;
        }
        return true;
    }
    void Run() {
        if (!CheckParameters()) {
            std::cerr << "Checking parameters failed!" << std::endl;
            return;
        }
        trace_size_ = FLAGS_num;
        key_trace_ = new RandomKeyTrace<KEY_TYPE, PAYLOAD_TYPE>(trace_size_,
                                                                FLAGS_is_seq);
        num_ = key_trace_->unique_ordered_keys_.size();
        FLAGS_num = key_trace_->unique_ordered_keys_.size();
        trace_size_ = key_trace_->unique_ordered_keys_.size();
        if (reads_ == 0) {
            reads_ = key_trace_->unique_ordered_keys_.size();
        }

        PrintHeader();
        // run benchmark
        bool print_hist = false;
        const char* benchmarks = FLAGS_benchmarks.c_str();
        /* int thread = FLAGS_worker_threads;
        uint32_t concurrent_count = std::thread::hardware_concurrency ();
        std::cout << "The number of hardware thread contexts: " <<
        concurrent_count
                  << "; current worker threads number: " << thread << std::endl;
        if (thread > concurrent_count) {
            thread = concurrent_count;
        } */
        while (benchmarks != nullptr) {
            int thread = FLAGS_worker_threads;

            void (Benchmark::*method)(ThreadState*) = nullptr;
            const char* sep = strchr(benchmarks, ',');
            std::string name;
            if (sep == nullptr) {
                name = benchmarks;
                benchmarks = nullptr;
            } else {
                name = std::string(benchmarks, sep - benchmarks);
                benchmarks = sep + 1;
            }

            if (name == "randomizeworkload") {
                key_trace_->Randomize();
            }

            if (name == "bulkload") {
                thread = 1;
                method = &Benchmark::DoBulkLoad;
            } else if (name == "readalltlxbtree") {
                method = &Benchmark::DoReadAllTlxbtree;
            } else if (name == "readtrace") {
                thread = 1;
                method = &Benchmark::DoReadTrace;
            } else if (name == "savetrace") {
                thread = 1;
                method = &Benchmark::DoSaveTrace;
            } else if (name == "format") {
                thread = 1;
                method = &Benchmark::DoFormat;
            } else if (name == "statistics") {
                thread = 1;
                method = &Benchmark::DoStatistics;
            } else if (name == "geninsertworkload") {
                thread = 1;
                method = &Benchmark::DoGenInsertWorkload;
            } else if (name == "genmixworkload") {
                thread = 1;
                method = &Benchmark::DoGenMixWorkload;
            } else if (name == "tlx_read_write") {
                method = &Benchmark::DoTlxBtreeReadWrite;
            }

            if (method != nullptr)
                RunBenchmark(thread, name, method, print_hist);
        }

        // lipp_olc_.thread_map.clear();
        std::cout << "[Run] End of run. " << std::endl;
    }

    void DoBulkLoad(ThreadState* thread) {
        std::cout << "Starting buck load... bulkload_keys_.size="
                  << key_trace_->bulkload_keys_.size() << std::endl;
        std::string index = FLAGS_benchmarks;

        size_t num_keys = key_trace_->bulkload_keys_.size();

        // btree
        if (index.find("btree")) {
            std::pair<KEY_TYPE, PAYLOAD_TYPE>* values =
                new std::pair<KEY_TYPE, PAYLOAD_TYPE>[num_keys];
            for (size_t i = 0; i < num_keys; ++i) {
                values[i].first = key_trace_->bulkload_keys_[i];
                values[i].second = key_trace_->bulkload_keys_[i] + 2;
            }

            // tlx_btree_map_
            if (index.find("tlx") != std::string::npos) {
                std::cout << "Initializing TLX Btree ..." << std::endl;
                for (size_t i = 0; i < num_keys; i++) {
                    tlx_btree_map_.insert(values[i]);
                }
            }
            delete[] values;
        }
    }

    void DoReadAllTlxbtree(ThreadState* thread) {
        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror("DoReadAllTlxbtree lack key_trace_ initialization.");
            return;
        }
        size_t interval = 0;
        size_t start_offset = 0;
        if (FLAGS_use_interval) {
            interval = std::ceil((double)reads_ / FLAGS_worker_threads);
            start_offset = thread->tid * interval;
        } else {
            interval = reads_;
            start_offset = 0;
        }
        auto key_iterator = key_trace_->read_iterate_between(
            start_offset, start_offset + interval);

        size_t not_find = 0;
        PAYLOAD_TYPE* ret;
        Duration duration(FLAGS_readtime, reads_);
        // std::cout << "readtime:" << FLAGS_readtime << "; reads_:" << reads_
        // << std::endl;
        thread->stats.Start();
        while (!duration.Done(batch) && key_iterator.Valid()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid(); j++) {
                size_t ikey = key_iterator.Next();
                auto ret = tlx_btree_map_.find(ikey);
                if (ret == tlx_btree_map_.end()) {
                    not_find++;
                }
            }
            thread->stats.FinishedBatchOp(j);
        }
        char buf[100];
        snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu)", interval,
                 not_find);

        thread->stats.AddMessage(buf);
    }

    void DoSaveTrace(ThreadState* thread) {
        auto starttime = std::chrono::system_clock::now();
        key_trace_->ToFile(FLAGS_tracefile);
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
    }

    void DoReadTrace(ThreadState* thread) {
        auto starttime = std::chrono::system_clock::now();
        bool result = key_trace_->FromFile(FLAGS_tracefile);
        if (!result) {
            std::cerr << "[DoReadTrace] Read trace failed! Exit the program!"
                      << std::endl;
            exit(1);
        }
        num_ = key_trace_->unique_ordered_keys_.size();
        FLAGS_num = key_trace_->unique_ordered_keys_.size();
        trace_size_ = key_trace_->unique_ordered_keys_.size();
        if (reads_ == 0) {
            reads_ = key_trace_->read_keys_.size();
        }
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        std::cout << "[DoReadTrace] File:" << FLAGS_tracefile
                  << "; num of keys:" << num_ << std::endl;
    }

    void DoGenInsertWorkload(ThreadState* thread) {
        std::cout
            << "Starting generating insert workload. Percentage of unique "
               "keys for first model building:"
            << FLAGS_bulk_load_ratio << std::endl;
        key_trace_->GenerateInsertWorkload(FLAGS_bulk_load_ratio);
    }

    void DoFormat(ThreadState* thread) {
        printf("Starting format\n");
        key_trace_->FormatKeys(FLAGS_bulk_load_ratio);
        reads_ = key_trace_->read_keys_.size();
    }

    void DoStatistics(ThreadState* thread) {
        std::cout << "All benchmarks have been completed!" << std::endl;
        // lisa_->Statistics ();
        // lisa_olc_->Statistics ();
        // For dubug
        // std::string output = "lisa_demo_trace_lines_info_0608.csv";
        // lisa_->SaveLinesInfoToFile (output, true);
    }

    void DoGenMixWorkload(ThreadState* thread) {
        std::cout << "Starting generating mix workload..." << std::endl;
        std::cout << "Read ratio:" << FLAGS_read_ratio
                  << "; Insert ratio:" << FLAGS_insert_ratio
                  << "; Update ratio:" << FLAGS_update_ratio
                  << "; Delete ratio:" << FLAGS_delete_ratio
                  << "; Scan ratio:" << FLAGS_scan_ratio << std::endl;

        key_trace_->GenerateMixWorkload(FLAGS_operation_num, FLAGS_read_ratio,
                                        FLAGS_insert_ratio, FLAGS_update_ratio,
                                        FLAGS_update_ratio, FLAGS_scan_ratio);
    }

    void DoTlxBtreeReadWrite(ThreadState* thread) {
        std::cout
            << "[DoTlxBtreeReadWrite] Info: tlx-btree read and write ...\n";

        uint64_t batch = FLAGS_batch;
        if (key_trace_ == nullptr) {
            perror("DoTlxBtreeReadWrite lack key_trace_ initialization.");
            return;
        }

        size_t not_find = 0;
        size_t interval = 0;
        size_t start_offset = 0;
        if (FLAGS_use_interval) {
            interval =
                std::ceil((double)(key_trace_->mix_workload_keys_.size()) /
                          FLAGS_worker_threads);
            start_offset = thread->tid * interval;
        } else {
            interval = key_trace_->mix_workload_keys_.size();
            start_offset = 0;
        }
        auto key_iterator = key_trace_->mix_workload_iterate_between(
            start_offset, start_offset + interval);
        PAYLOAD_TYPE val;
        bool ret;
        Duration duration(FLAGS_readtime,
                          key_trace_->mix_workload_keys_.size());

        thread->stats.Start();
        while (!duration.Done(batch) && key_iterator.Valid()) {
            uint64_t j = 0;
            for (; j < batch && key_iterator.Valid(); j++) {
                auto pair = key_iterator.NextOpPair();
                KEY_TYPE ikey = pair.first;
                Operation op = pair.second;
                if (op == kRead) {
                    auto res = tlx_btree_map_.find(ikey);
                    /* if (res == tlx_btree_map_.end ()) {
                        not_find++;
                    } */
                } else if (op == kInsert) {
                    tlx_btree_map_.insert(std::make_pair(ikey, ikey + 3));
                } else if (op == kUpdate) {
                    tlx_btree_map_.insert(std::make_pair(ikey, ikey + 5));
                } else if (op == kDelete) {
                    tlx_btree_map_.erase(ikey);
                } else if (op == kScan) {
                    /* auto scan_size = btree_olc_.range_scan_by_size (
                        key_low_bound, static_cast<uint32_t> (key_num), result);
                     */
                }
            }
            thread->stats.FinishedBatchOp(j);
        }
        char buf[100];
        snprintf(buf, sizeof(buf), "(num: %lu, not find: %lu)", interval,
                 not_find);

        thread->stats.AddMessage(buf);
    }

   private:
    struct ThreadArg {
        Benchmark* bm;
        SharedState* shared;
        ThreadState* thread;
        void (Benchmark::*method)(ThreadState*);
    };

    static void ThreadBody(void* v) {
        ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
        SharedState* shared = arg->shared;
        ThreadState* thread = arg->thread;
        {
            std::unique_lock<std::mutex> lck(shared->mu);
            shared->num_initialized++;
            if (shared->num_initialized >= shared->total) {
                shared->cv.notify_all();
            }
            while (!shared->start) {
                shared->cv.wait(lck);
            }
        }

        thread->stats.Start();
        (arg->bm->*(arg->method))(thread);
        thread->stats.Stop();

        {
            std::unique_lock<std::mutex> lck(shared->mu);
            shared->num_done++;
            if (shared->num_done >= shared->total) {
                shared->cv.notify_all();
            }
        }
    }

    void RunBenchmark(int thread_num, const std::string& name,
                      void (Benchmark::*method)(ThreadState*),
                      bool print_hist) {
        SharedState shared(thread_num);
        ThreadArg* arg = new ThreadArg[thread_num];
        std::thread server_threads[thread_num];
        std::vector<int> thread_mapping(
            {30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0});
        for (int i = 0; i < thread_num; i++) {
            arg[i].bm = this;
            arg[i].method = method;
            arg[i].shared = &shared;
            arg[i].thread = new ThreadState(i);
            arg[i].thread->shared = &shared;
            server_threads[i] = std::thread(ThreadBody, &arg[i]);

            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(thread_mapping[i], &cpuset);
            int rc = pthread_setaffinity_np(server_threads[i].native_handle(),
                                            sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc
                          << "\n";
            }
        }

        std::unique_lock<std::mutex> lck(shared.mu);
        while (shared.num_initialized < thread_num) {
            shared.cv.wait(lck);
        }

        shared.start = true;
        shared.cv.notify_all();
        while (shared.num_done < thread_num) {
            shared.cv.wait(lck);
        }

        for (int i = 1; i < thread_num; i++) {
            arg[0].thread->stats.Merge(arg[i].thread->stats);
        }
        arg[0].thread->stats.Report(name, print_hist);

        for (auto& th : server_threads) th.join();

        for (int i = 0; i < thread_num; i++) {
            delete arg[i].thread;
        }
        delete[] arg;
    }

    void PrintEnvironment() {
#if defined(__linux)
        time_t now = time(nullptr);
        fprintf(stderr, "Date:                  %s",
                ctime(&now));  // ctime() adds newline

        FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
        if (cpuinfo != nullptr) {
            char line[1000];
            int num_cpus = 0;
            std::string cpu_type;
            std::string cache_size;
            while (fgets(line, sizeof(line), cpuinfo) != nullptr) {
                const char* sep = strchr(line, ':');
                if (sep == nullptr) {
                    continue;
                }
                std::string key = TrimSpace(std::string(line, sep - 1 - line));
                std::string val = TrimSpace(std::string(sep + 1));
                if (key == "model name") {
                    ++num_cpus;
                    cpu_type = val;
                } else if (key == "cache size") {
                    cache_size = val;
                }
            }
            fclose(cpuinfo);
            fprintf(stderr, "CPU:                   %d * %s\n", num_cpus,
                    cpu_type.c_str());
            fprintf(stderr, "CPUCache:              %s\n", cache_size.c_str());
        }
#endif
    }

    void PrintHeader() {
        fprintf(stdout, "------------------------------------------------\n");
        PrintEnvironment();
        fprintf(stdout, "LISA\n");
        fprintf(stdout, "Entries:               %lu\n", (uint64_t)num_);
        fprintf(stdout, "Trace size:            %lu\n", (uint64_t)trace_size_);
        fprintf(stdout, "Read:                  %lu \n", (uint64_t)FLAGS_read);
        fprintf(stdout, "Write:                 %lu \n", (uint64_t)FLAGS_write);
        fprintf(stdout, "Batch:                 %lu \n", (uint64_t)FLAGS_batch);
        fprintf(stdout, "Thread:                %lu \n",
                (uint64_t)FLAGS_worker_threads);
        fprintf(stdout, "Report interval:       %lu s\n",
                (uint64_t)FLAGS_report_interval);
        fprintf(stdout, "Stats interval:        %lu records\n",
                (uint64_t)FLAGS_stats_interval);
        fprintf(stdout, "benchmarks:            %s\n",
                FLAGS_benchmarks.c_str());
        fprintf(stdout, "------------------------------------------------\n");
    }
};

int main(int argc, char* argv[]) {
    // ParseCommandLineFlags(&argc, &argv, true);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    Benchmark<uint64_t, uint64_t> benchmark;
    benchmark.Run();
    std::cout << "Finish this experiment!" << std::endl;
    // Clean up
    gflags::ShutDownCommandLineFlags();
    return 0;
}