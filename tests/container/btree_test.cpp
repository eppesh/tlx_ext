/*******************************************************************************
 * tests/container/btree_test.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/container/btree_map.hpp>
#include <tlx/container/btree_multimap.hpp>
#include <tlx/container/btree_multiset.hpp>
#include <tlx/container/btree_set.hpp>

#include <tlx/die.hpp>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <csignal>
#include <unistd.h>
#include <iostream>
#include <set>
#include <vector>
#include <algorithm>
#include <cassert>
#include <iostream>
#include <random>
#include <string>
#include <utility>
#include <map>
#include <execinfo.h>
#include <cxxabi.h>
#include <sstream>
#include <stack>

#if TLX_MORE_TESTS
static const bool tlx_more_tests = true;
#else
static const bool tlx_more_tests = false;
#endif

static const bool test_multi = false;
static const bool multithread = true;
static const auto seed = std::random_device{}();

std::string format_current_time() {
    // Get the current time from system_clock
    auto now = std::chrono::system_clock::now();

    // Convert to time_t to get calendar time
    std::time_t time_t_now = std::chrono::system_clock::to_time_t(now);

    // Convert time_t to tm for formatting
    std::tm tm_now = *std::localtime(&time_t_now);

    // Get the duration since the epoch
    auto duration_since_epoch = now.time_since_epoch();

    // Extract seconds and nanoseconds from the duration
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration_since_epoch);
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration_since_epoch) - seconds;

    // Format the output string
    std::stringstream ss;
    ss << std::put_time(&tm_now, "%Y-%m-%d %H:%M:%S");
    ss << '.' << std::setw(6) << std::setfill('0') << microseconds.count();

    return ss.str();
}

void split_sym(char *input, std::vector<std::string> *resultp) {
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
        } else if ((ch == ')' && !bracket_stack.empty() && bracket_stack.top() == '(') ||
                   (ch == ']' && !bracket_stack.empty() && bracket_stack.top() == '[') ||
                   (ch == '}' && !bracket_stack.empty() && bracket_stack.top() == '{') ||
                   (ch == '>' && !bracket_stack.empty() && bracket_stack.top() == '<')) {
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
    ssize_t pos = clean_input.find_last_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_");
    auto res = clean_input.substr(pos + 1);
    return res;
}

std::string stack_sym(int start_stack, int num_stacks) {
    const int NUM = 10;
    void * addrs[NUM];
    int num_frames = backtrace(addrs, NUM);
    char **strs = backtrace_symbols(addrs, num_frames);
    int status;
    std::vector<std::string> names;
    std::string s = "";

    for (int i = start_stack; i < std::min(start_stack + num_stacks, num_frames); ++i) {
        split_sym(strs[i], &names);
        char *demangled_name = abi::__cxa_demangle(names[3].c_str(), 0, 0, &status);
        std::string func_name = extract_func_name(demangled_name);
        std::stringstream stream;
        stream << func_name << '(' << std::hex << addrs[i] << ") ";
        s += stream.str();
        free(demangled_name);
    }
    return s;
}

namespace tlx{
int seq = 0; // TODO
}

/******************************************************************************/
// Instantiation Tests

template class tlx::btree_set<unsigned int>;
template class tlx::btree_map<int, double>;
template class tlx::btree_multiset<int>;
template class tlx::btree_multimap<int, int>;

/******************************************************************************/
// Simple Tests

template <int Slots>
struct SimpleTest {
    template <typename KeyType>
    struct traits_nodebug : tlx::btree_default_traits<KeyType, KeyType> {
        static const bool self_verify = true;
        static const bool debug = false;

        static const int leaf_slots = Slots;
        static const int inner_slots = Slots;
    };

    static void test_empty() {
        typedef tlx::btree_set<
            unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> >
        btree_type;


        btree_type bt, bt2;
        bt.verify();

        die_unless(bt.erase(42) == false);

        die_unless(bt == bt2);
    }

    static void test_set_insert_erase_3200() {
       if (test_multi) {
            typedef tlx::btree_multiset<
                unsigned int,
                std::less<unsigned int>, traits_nodebug<unsigned int> >
            btree_type;

            btree_type bt;
            bt.verify();

            srand(seed);
            for (unsigned int i = 0; i < 3200; i++)
            {
                die_unless(bt.size() == i);
                bt.insert(rand() % 100);
                die_unless(bt.size() == i + 1);
            }

            srand(seed);
            for (unsigned int i = 0; i < 3200; i++)
            {
                die_unless(bt.size() == 3200 - i);
                die_unless(bt.erase_one(rand() % 100));
                die_unless(bt.size() == 3200 - i - 1);
            }

            die_unless(bt.empty());
        } else {
            // from frozenca's btree tests
            typedef tlx::btree_set<
                unsigned int,
                std::less<unsigned int>, traits_nodebug<unsigned int> >
            btree_type;

            btree_type btree;
            int n = 100;

            //std::cout << __func__ << " seed: " << seed << std::endl;
            std::mt19937 gen(seed);

            std::vector<int> v(n);
            std::iota(v.begin(), v.end(), 0);

            unsigned long size = 0;
            // random insert
            std::ranges::shuffle(v, gen);
            for (auto num : v) {
                die_unless(btree.insert(num).second);
                die_unless(btree.size() == ++size);
            }

            btree.verify();

            // random lookup
            std::ranges::shuffle(v, gen);
            for (auto num : v) {
              die_unless(btree.exists(num));
            }

            // random erase
            std::ranges::shuffle(v, gen);
            for (auto num : v) {
                bool res = btree.erase(num);
                die_unless(res);
                die_unless(btree.size() == --size);
            }
        }
    }

    static void test_set_insert_erase_3200_descending() {
       if (test_multi) {
            typedef tlx::btree_multiset<
                unsigned int,
                std::greater<unsigned int>, traits_nodebug<unsigned int> >
            btree_type;

            btree_type bt;

            srand(seed);
            for (unsigned int i = 0; i < 3200; i++)
            {
                die_unless(bt.size() == i);
                bt.insert(rand() % 100);
                die_unless(bt.size() == i + 1);
            }

            srand(seed);
            for (unsigned int i = 0; i < 3200; i++)
            {
                die_unless(bt.size() == 3200 - i);
                die_unless(bt.erase_one(rand() % 100));
                die_unless(bt.size() == 3200 - i - 1);
            }

            die_unless(bt.empty());
        } else {
            // from frozenca's btree tests
            typedef tlx::btree_set<
                unsigned int,
                std::greater<unsigned int>, traits_nodebug<unsigned int> >
            btree_type;

            btree_type btree;
            int n = 100;

            std::mt19937 gen(seed);

            std::vector<int> v(n);
            std::iota(v.begin(), v.end(), 0);

            unsigned long size = 0;

            // random insert
            std::ranges::shuffle(v, gen);
            for (auto num : v) {
              die_unless(btree.insert(num).second);
              die_unless(btree.size() == ++size);
            }

            btree.verify();

            // random lookup
            std::ranges::shuffle(v, gen);
            for (auto num : v) {
              die_unless(btree.exists(num));
            }

            // random erase
            std::ranges::shuffle(v, gen);
            for (auto num : v) {
              die_unless(btree.erase(num));
              die_unless(btree.size() == --size);
            }
        }
    }

    static void test_map_insert_erase_3200() {
        if (test_multi) {
            typedef tlx::btree_multimap<
                    unsigned int, std::string,
                    std::less<unsigned int>, traits_nodebug<unsigned int> >
                btree_type;

            btree_type bt;

            srand(seed);
            for (unsigned int i = 0; i < 3200; i++)
            {
                die_unless(bt.size() == i);
                bt.insert2(rand() % 100, "101");
                die_unless(bt.size() == i + 1);
            }

            srand(seed);
            for (unsigned int i = 0; i < 3200; i++)
            {
                die_unless(bt.size() == 3200 - i);
                die_unless(bt.erase_one(rand() % 100));
                die_unless(bt.size() == 3200 - i - 1);
            }

            die_unless(bt.empty());
            bt.verify();
        } else {
            typedef tlx::btree_map<
                    std::string, unsigned int,
                    std::less<std::string>, traits_nodebug<unsigned int> >
                btree_type;

            btree_type btree;

            // frozenca tests
            btree["asd"] = 3;
            btree["a"] = 6;
            btree["bbb"] = 9;
            btree["asdf"] = 8;
            btree["asdf"] = 333;
            die_unless(btree["asdf"] == 333);

            btree.insert2("asdfgh", 200);
            die_unless(btree["asdfgh"] == 200);

            btree.verify();
            die_unless(btree.size() == 5);
        }
    }

    static void test_map_insert_erase_3200_descending() {
        if (test_multi) {
            typedef tlx::btree_multimap<
                    unsigned int, std::string,
                    std::greater<unsigned int>, traits_nodebug<unsigned int> >
                btree_type;

            btree_type bt;

            srand(seed);
            for (unsigned int i = 0; i < 3200; i++)
            {
                die_unless(bt.size() == i);
                bt.insert2(rand() % 100, "101");
                die_unless(bt.size() == i + 1);
            }

            srand(seed);
            for (unsigned int i = 0; i < 3200; i++)
            {
                die_unless(bt.size() == 3200 - i);
                die_unless(bt.erase_one(rand() % 100));
                die_unless(bt.size() == 3200 - i - 1);
            }

            die_unless(bt.empty());
            bt.verify();
        } else {
            typedef tlx::btree_map<
                    std::string, unsigned int,
                    std::greater<std::string>, traits_nodebug<unsigned int> >
                btree_type;

            btree_type btree;

            // frozenca tests
            btree["asd"] = 3;
            btree["a"] = 6;
            btree["bbb"] = 9;
            btree["asdf"] = 8;
            btree["asdf"] = 333;
            die_unless(btree["asdf"] == 333);

            btree.insert2("asdfgh", 200);
            die_unless(btree["asdfgh"] == 200);

            btree.verify();
            die_unless(btree.size() == 5);
        }
    }

    static void test2_map_insert_erase_strings() {
        if (!test_multi) return;

        typedef tlx::btree_multimap<
                std::string, unsigned int,
                std::less<std::string>, traits_nodebug<std::string> >
            btree_type;

        std::string letters = "abcdefghijklmnopqrstuvwxyz";

        btree_type bt;

        for (unsigned int a = 0; a < letters.size(); ++a)
        {
            for (unsigned int b = 0; b < letters.size(); ++b)
            {
                bt.insert2(std::string(1, letters[a]) + letters[b],
                           static_cast<unsigned int>(a * letters.size() + b));
            }
        }

        for (unsigned int b = 0; b < letters.size(); ++b)
        {
            for (unsigned int a = 0; a < letters.size(); ++a)
            {
                std::string key = std::string(1, letters[a]) + letters[b];

                die_unless(bt.find(key)->second == a * letters.size() + b);
                die_unless(bt.erase_one(key));
            }
        }

        die_unless(bt.empty());
        bt.verify();
    }

    static void test_map_100000_uint64() {
        tlx::btree_map<std::uint64_t, std::uint8_t> bt;

        for (std::uint64_t i = 10; i < 100000; ++i)
        {
            std::uint64_t key = i % 1000;

            if (bt.find(key) == bt.end())
            {
                bt.insert(std::make_pair(key, key % 100));
            }
        }

        die_unless(bt.size() == 1000);
    }

    static void test_multiset_100000_uint32() {
        if (!test_multi) return;

        tlx::btree_multiset<std::uint32_t> bt;

        for (std::uint64_t i = 0; i < 100000; ++i)
        {
            std::uint32_t key = i % 1000;

            bt.insert(key);
        }

        die_unless(bt.size() == 100000);
    }

    SimpleTest() {
        test_empty();
        test_set_insert_erase_3200();
        test_set_insert_erase_3200_descending();
        test_map_insert_erase_3200();
        test_map_insert_erase_3200_descending();
        test2_map_insert_erase_strings();
        test_map_100000_uint64();
        test_multiset_100000_uint32();
    }
};

void test_simple() {
    // test binary search on different slot sizes
    if (tlx_more_tests) {
        SimpleTest<8>();
        SimpleTest<9>();
        SimpleTest<10>();
        SimpleTest<11>();
        SimpleTest<12>();
        SimpleTest<13>();
        SimpleTest<14>();
        SimpleTest<15>();
    }
    SimpleTest<16>();
    if (tlx_more_tests) {
        SimpleTest<17>();
        SimpleTest<19>();
        SimpleTest<20>();
        SimpleTest<21>();
        SimpleTest<23>();
        SimpleTest<24>();
        SimpleTest<32>();
        SimpleTest<48>();
        SimpleTest<63>();
        SimpleTest<64>();
        SimpleTest<65>();
        SimpleTest<101>();
        SimpleTest<203>();
    }
}

/******************************************************************************/
// Large Test

template <typename KeyType>
struct traits_nodebug : tlx::btree_default_traits<KeyType, KeyType> {
    static const bool self_verify = false;
    static const bool debug = false;

    static const int leaf_slots = 8;
    static const int inner_slots = 8;
};

void test_large_multiset(const unsigned int insnum, const unsigned int modulo) {
    if (!test_multi) return;

    typedef tlx::btree_multiset<
            unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type bt;

    typedef std::multiset<unsigned int> multiset_type;
    multiset_type set;

    // *** insert
    srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        die_unless(bt.size() == set.size());
        bt.insert(k);
        set.insert(k);
        die_unless(bt.count(k) == set.count(k));

        die_unless(bt.size() == set.size());
    }

    die_unless(bt.size() == insnum);

    // *** iterate
    btree_type::iterator bi = bt.begin();
    multiset_type::const_iterator si = set.begin();
    for ( ; bi != bt.end() && si != set.end(); ++bi, ++si)
    {
        die_unless(*si == bi.key());
    }
    die_unless(bi == bt.end());
    die_unless(si == set.end());

    // *** existance
    srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        die_unless(bt.exists(k));
    }

    // *** counting
    srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        die_unless(bt.count(k) == set.count(k));
    }

    // *** deletion
    srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        if (set.find(k) != set.end())
        {
            die_unless(bt.size() == set.size());

            die_unless(bt.exists(k));
            die_unless(bt.erase_one(k));
            set.erase(set.find(k));

            die_unless(bt.size() == set.size());
            die_unless(std::equal(bt.begin(), bt.end(), set.begin()));
        }
    }

    die_unless(bt.empty());
    die_unless(set.empty());
}

/*void test_large_set(const unsigned int insnum, const unsigned int modulo) {
    // from frozenca's btree tests
    typedef tlx::btree_set<
        unsigned int,
        std::greater<unsigned int>, traits_nodebug<unsigned int> >
    btree_type;

    btree_type btree;
    std::mt19937 gen(std::random_device{}());
    std::vector<int> v(insnum);
    std::iota(v.begin(), v.end(), 0);
    unsigned long size = 0;
    // random insert
    std::ranges::shuffle(v, gen);
    for (auto num : v) {
      die_unless(btree.insert(num%modulo).second);
      die_unless(btree.size() == ++size);
    }

    btree.verify();
    // random lookup
    std::ranges::shuffle(v, gen);
    for (auto num : v) {
      die_unless(btree.exists(num%modulo));
    }

    // random erase
    std::ranges::shuffle(v, gen);
    for (auto num : v) {
      die_unless(btree.erase(num));
      die_unless(btree.size() == --size);
    }
}*/

// TODO currently only multiset
void test_large() {
    test_large_multiset(320, 1000);
    test_large_multiset(320, 10000);
    test_large_multiset(3200, 10);
    test_large_multiset(3200, 100);
    test_large_multiset(3200, 1000);
    test_large_multiset(3200, 10000);
    test_large_multiset(32000, 10000);
}

void test_large_sequence() {

#if test_multi
    typedef tlx::btree_multiset<
            unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    typedef std::multiset<unsigned int> stdset_type;
#else
    typedef tlx::btree_set<
        unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    typedef std::set<unsigned int> stdset_type;
#endif

    btree_type bt;

    const unsigned int insnum = 10000;

    stdset_type set;

    // *** insert
    //srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = i;

        die_unless(bt.size() == set.size());
        bt.insert(k);
        set.insert(k);
        die_unless(bt.count(k) == set.count(k));

        die_unless(bt.size() == set.size());
    }

    die_unless(bt.size() == insnum);

    // *** iterate
    btree_type::iterator bi = bt.begin();
    stdset_type::const_iterator si = set.begin();
    for ( ; bi != bt.end() && si != set.end(); ++bi, ++si)
    {
        die_unless(*si == bi.key());
    }
    die_unless(bi == bt.end());
    die_unless(si == set.end());

    // *** existance
    //srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = i;

        die_unless(bt.exists(k));
    }

    // *** counting
    //srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = i;

        die_unless(bt.count(k) == set.count(k));
    }

    // *** deletion
    //srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = i;

        if (set.find(k) != set.end())
        {
            die_unless(bt.size() == set.size());

            die_unless(bt.exists(k));
            die_unless(bt.erase_one(k));
            set.erase(set.find(k));

            die_unless(bt.size() == set.size());
            die_unless(std::equal(bt.begin(), bt.end(), set.begin()));
        }
    }

    die_unless(bt.empty());
    die_unless(set.empty());
}

/******************************************************************************/
// Upper/Lower Bound Tests

void test_bounds_multimap(const unsigned int insnum, const unsigned int modulo) {
    if (!test_multi) return;

    typedef tlx::btree_multimap<
            unsigned int, unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;
    btree_type bt;

    typedef std::multiset<unsigned int> multiset_type;
    multiset_type set;

    // *** insert
    srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;
        unsigned int v = 234;

        die_unless(bt.size() == set.size());
        bt.insert2(k, v);
        set.insert(k);
        die_unless(bt.count(k) == set.count(k));

        die_unless(bt.size() == set.size());
    }

    die_unless(bt.size() == insnum);

    // *** iterate
    {
        btree_type::iterator bi = bt.begin();
        multiset_type::const_iterator si = set.begin();
        for ( ; bi != bt.end() && si != set.end(); ++bi, ++si)
        {
            die_unless(*si == bi.key());
        }
        die_unless(bi == bt.end());
        die_unless(si == set.end());
    }

    // *** existance
    srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        die_unless(bt.exists(k));
    }

    // *** counting
    srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        die_unless(bt.count(k) == set.count(k));
    }

    // *** lower_bound
    for (unsigned int k = 0; k < modulo + 100; k++)
    {
        multiset_type::const_iterator si = set.lower_bound(k);
        btree_type::const_iterator bi = bt.lower_bound(k);

        if (bi == bt.end())
            die_unless(si == set.end());
        else if (si == set.end())
            die_unless(bi == bt.end());
        else
            die_unless(*si == bi.key());
    }

    // *** upper_bound
    for (unsigned int k = 0; k < modulo + 100; k++)
    {
        multiset_type::const_iterator si = set.upper_bound(k);
        btree_type::const_iterator bi = bt.upper_bound(k);

        if (bi == bt.end())
            die_unless(si == set.end());
        else if (si == set.end())
            die_unless(bi == bt.end());
        else
            die_unless(*si == bi.key());
    }

    // *** equal_range
    for (unsigned int k = 0; k < modulo + 100; k++)
    {
        std::pair<multiset_type::const_iterator, multiset_type::const_iterator> si = set.equal_range(k);
        std::pair<btree_type::const_iterator, btree_type::const_iterator> bi = bt.equal_range(k);

        if (bi.first == bt.end())
            die_unless(si.first == set.end());
        else if (si.first == set.end())
            die_unless(bi.first == bt.end());
        else
            die_unless(*si.first == bi.first.key());

        if (bi.second == bt.end())
            die_unless(si.second == set.end());
        else if (si.second == set.end())
            die_unless(bi.second == bt.end());
        else
            die_unless(*si.second == bi.second.key());
    }

    // *** deletion
    srand(seed);
    for (unsigned int i = 0; i < insnum; i++)
    {
        unsigned int k = rand() % modulo;

        if (set.find(k) != set.end())
        {
            die_unless(bt.size() == set.size());

            die_unless(bt.exists(k));
            die_unless(bt.erase_one(k));
            set.erase(set.find(k));

            die_unless(bt.size() == set.size());
        }
    }

    die_unless(bt.empty());
    die_unless(set.empty());
}

void test_bounds() {
    test_bounds_multimap(3200, 10);
    test_bounds_multimap(320, 1000);
}

/******************************************************************************/
// Test Iterators

void test_iterator1() {
    if (!test_multi) return;

    typedef tlx::btree_multiset<
            unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::vector<unsigned int> vector;

    srand(seed);
    for (unsigned int i = 0; i < 3200; i++)
    {
        vector.push_back(rand() % 1000);
    }

    die_unless(vector.size() == 3200);

    // test construction and insert(iter, iter) function
    btree_type bt(vector.begin(), vector.end());

    die_unless(bt.size() == 3200);

    // copy for later use
    btree_type bt2 = bt;

    // empty out the first bt
    srand(seed);
    for (unsigned int i = 0; i < 3200; i++)
    {
        die_unless(bt.size() == 3200 - i);
        die_unless(bt.erase_one(rand() % 1000));
        die_unless(bt.size() == 3200 - i - 1);
    }

    die_unless(bt.empty());

    // copy btree values back to a vector

    std::vector<unsigned int> vector2;
    vector2.assign(bt2.begin(), bt2.end());

    // afer sorting the vector, the two must be the same
    std::sort(vector.begin(), vector.end());

    die_unless(vector == vector2);

    // test reverse iterator
    vector2.clear();
    vector2.assign(bt2.rbegin(), bt2.rend());

    std::reverse(vector.begin(), vector.end());

    btree_type::reverse_iterator ri = bt2.rbegin();
    for (unsigned int i = 0; i < vector2.size(); ++i)
    {
        die_unless(vector[i] == vector2[i]);
        die_unless(vector[i] == *ri);

        ri++;
    }

    die_unless(ri == bt2.rend());
}

void test_iterator2() {
    if (!test_multi) return;

    typedef tlx::btree_multimap<
            unsigned int, unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::vector<btree_type::value_type> vector;

    srand(seed);
    for (unsigned int i = 0; i < 3200; i++)
    {
        vector.push_back(btree_type::value_type(rand() % 1000, 0));
    }

    die_unless(vector.size() == 3200);

    // test construction and insert(iter, iter) function
    btree_type bt(vector.begin(), vector.end());

    die_unless(bt.size() == 3200);

    // copy for later use
    btree_type bt2 = bt;

    // empty out the first bt
    srand(seed);
    for (unsigned int i = 0; i < 3200; i++)
    {
        die_unless(bt.size() == 3200 - i);
        die_unless(bt.erase_one(rand() % 1000));
        die_unless(bt.size() == 3200 - i - 1);
    }

    die_unless(bt.empty());

    // copy btree values back to a vector

    std::vector<btree_type::value_type> vector2;
    vector2.assign(bt2.begin(), bt2.end());

    // afer sorting the vector, the two must be the same
    std::sort(vector.begin(), vector.end());

    die_unless(vector == vector2);

    // test reverse iterator
    vector2.clear();
    vector2.assign(bt2.rbegin(), bt2.rend());

    std::reverse(vector.begin(), vector.end());

    btree_type::reverse_iterator ri = bt2.rbegin();
    for (unsigned int i = 0; i < vector2.size(); ++i, ++ri)
    {
        die_unless(vector[i].first == vector2[i].first);
        die_unless(vector[i].first == ri->first);
        die_unless(vector[i].second == ri->second);
    }

    die_unless(ri == bt2.rend());
}

void test_iterator3() {
    typedef tlx::btree_map<
            unsigned int, unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type map;

    unsigned int maxnum = 1000;

    for (unsigned int i = 0; i < maxnum; ++i)
    {
        map.insert(std::make_pair(i, i * 3));
    }

    {
        // test iterator prefix++
        unsigned int nownum = 0;

        for (btree_type::iterator i = map.begin();
             i != map.end(); ++i)
        {
            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);

            nownum++;
        }

        die_unless(nownum == maxnum);
    }

    {
        // test iterator prefix--
        unsigned int nownum = maxnum;

        btree_type::iterator i;
        for (i = --map.end(); i != map.begin(); --i)
        {
            nownum--;

            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);
        }

        nownum--;

        die_unless(nownum == i->first);
        die_unless(nownum * 3 == i->second);

        die_unless(nownum == 0);
    }

    {
        // test const_iterator prefix++
        unsigned int nownum = 0;

        for (btree_type::const_iterator i = map.begin();
             i != map.end(); ++i)
        {
            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);

            nownum++;
        }

        die_unless(nownum == maxnum);
    }

    {
        // test const_iterator prefix--
        unsigned int nownum = maxnum;

        btree_type::const_iterator i;
        for (i = --map.end(); i != map.begin(); --i)
        {
            nownum--;

            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);
        }

        nownum--;

        die_unless(nownum == i->first);
        die_unless(nownum * 3 == i->second);

        die_unless(nownum == 0);
    }

    {
        // test reverse_iterator prefix++
        unsigned int nownum = maxnum;

        for (btree_type::reverse_iterator i = map.rbegin();
             i != map.rend(); ++i)
        {
            nownum--;

            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);
        }

        die_unless(nownum == 0);
    }

    {
        // test reverse_iterator prefix--
        unsigned int nownum = 0;

        btree_type::reverse_iterator i;
        for (i = --map.rend(); i != map.rbegin(); --i)
        {
            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);

            nownum++;
        }

        die_unless(nownum == i->first);
        die_unless(nownum * 3 == i->second);

        nownum++;

        die_unless(nownum == maxnum);
    }

    {
        // test const_reverse_iterator prefix++
        unsigned int nownum = maxnum;

        for (btree_type::const_reverse_iterator i = map.rbegin();
             i != map.rend(); ++i)
        {
            nownum--;

            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);
        }

        die_unless(nownum == 0);
    }

    {
        // test const_reverse_iterator prefix--
        unsigned int nownum = 0;

        btree_type::const_reverse_iterator i;
        for (i = --map.rend(); i != map.rbegin(); --i)
        {
            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);

            nownum++;
        }

        die_unless(nownum == i->first);
        die_unless(nownum * 3 == i->second);

        nownum++;

        die_unless(nownum == maxnum);
    }

    // postfix

    {
        // test iterator postfix++
        unsigned int nownum = 0;

        for (btree_type::iterator i = map.begin();
             i != map.end(); i++)
        {
            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);

            nownum++;
        }

        die_unless(nownum == maxnum);
    }

    {
        // test iterator postfix--
        unsigned int nownum = maxnum;

        btree_type::iterator i;
        for (i = --map.end(); i != map.begin(); i--)
        {
            nownum--;

            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);
        }

        nownum--;

        die_unless(nownum == i->first);
        die_unless(nownum * 3 == i->second);

        die_unless(nownum == 0);
    }

    {
        // test const_iterator postfix++
        unsigned int nownum = 0;

        for (btree_type::const_iterator i = map.begin();
             i != map.end(); i++)
        {
            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);

            nownum++;
        }

        die_unless(nownum == maxnum);
    }

    {
        // test const_iterator postfix--
        unsigned int nownum = maxnum;

        btree_type::const_iterator i;
        for (i = --map.end(); i != map.begin(); i--)
        {
            nownum--;

            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);
        }

        nownum--;

        die_unless(nownum == i->first);
        die_unless(nownum * 3 == i->second);

        die_unless(nownum == 0);
    }

    {
        // test reverse_iterator postfix++
        unsigned int nownum = maxnum;

        for (btree_type::reverse_iterator i = map.rbegin();
             i != map.rend(); i++)
        {
            nownum--;

            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);
        }

        die_unless(nownum == 0);
    }

    {
        // test reverse_iterator postfix--
        unsigned int nownum = 0;

        btree_type::reverse_iterator i;
        for (i = --map.rend(); i != map.rbegin(); i--)
        {
            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);

            nownum++;
        }

        die_unless(nownum == i->first);
        die_unless(nownum * 3 == i->second);

        nownum++;

        die_unless(nownum == maxnum);
    }

    {
        // test const_reverse_iterator postfix++
        unsigned int nownum = maxnum;

        for (btree_type::const_reverse_iterator i = map.rbegin();
             i != map.rend(); i++)
        {
            nownum--;

            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);
        }

        die_unless(nownum == 0);
    }

    {
        // test const_reverse_iterator postfix--
        unsigned int nownum = 0;

        btree_type::const_reverse_iterator i;
        for (i = --map.rend(); i != map.rbegin(); i--)
        {
            die_unless(nownum == i->first);
            die_unless(nownum * 3 == i->second);

            nownum++;
        }

        die_unless(nownum == i->first);
        die_unless(nownum * 3 == i->second);

        nownum++;

        die_unless(nownum == maxnum);
    }
}

void test_iterator4() {
    typedef tlx::btree_set<
            unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type set;

    unsigned int maxnum = 1000;

    for (unsigned int i = 0; i < maxnum; ++i)
    {
        set.insert(i);
    }

    {
        // test iterator prefix++
        unsigned int nownum = 0;

        for (btree_type::iterator i = set.begin();
             i != set.end(); ++i)
        {
            die_unless(nownum == *i);
            nownum++;
        }

        die_unless(nownum == maxnum);
    }

    {
        // test iterator prefix--
        unsigned int nownum = maxnum;

        btree_type::iterator i;
        for (i = --set.end(); i != set.begin(); --i)
        {
            die_unless(--nownum == *i);
        }

        die_unless(--nownum == *i);

        die_unless(nownum == 0);
    }

    {
        // test const_iterator prefix++
        unsigned int nownum = 0;

        for (btree_type::const_iterator i = set.begin();
             i != set.end(); ++i)
        {
            die_unless(nownum++ == *i);
        }

        die_unless(nownum == maxnum);
    }

    {
        // test const_iterator prefix--
        unsigned int nownum = maxnum;

        btree_type::const_iterator i;
        for (i = --set.end(); i != set.begin(); --i)
        {
            die_unless(--nownum == *i);
        }

        die_unless(--nownum == *i);

        die_unless(nownum == 0);
    }

    {
        // test reverse_iterator prefix++
        unsigned int nownum = maxnum;

        for (btree_type::reverse_iterator i = set.rbegin();
             i != set.rend(); ++i)
        {
            die_unless(--nownum == *i);
        }

        die_unless(nownum == 0);
    }

    {
        // test reverse_iterator prefix--
        unsigned int nownum = 0;

        btree_type::reverse_iterator i;
        for (i = --set.rend(); i != set.rbegin(); --i)
        {
            die_unless(nownum++ == *i);
        }

        die_unless(nownum++ == *i);

        die_unless(nownum == maxnum);
    }

    {
        // test const_reverse_iterator prefix++
        unsigned int nownum = maxnum;

        for (btree_type::const_reverse_iterator i = set.rbegin();
             i != set.rend(); ++i)
        {
            die_unless(--nownum == *i);
        }

        die_unless(nownum == 0);
    }

    {
        // test const_reverse_iterator prefix--
        unsigned int nownum = 0;

        btree_type::const_reverse_iterator i;
        for (i = --set.rend(); i != set.rbegin(); --i)
        {
            die_unless(nownum++ == *i);
        }

        die_unless(nownum++ == *i);

        die_unless(nownum == maxnum);
    }

    // postfix

    {
        // test iterator postfix++
        unsigned int nownum = 0;

        for (btree_type::iterator i = set.begin();
             i != set.end(); i++)
        {
            die_unless(nownum++ == *i);
        }

        die_unless(nownum == maxnum);
    }

    {
        // test iterator postfix--
        unsigned int nownum = maxnum;

        btree_type::iterator i;
        for (i = --set.end(); i != set.begin(); i--)
        {

            die_unless(--nownum == *i);
        }

        die_unless(--nownum == *i);

        die_unless(nownum == 0);
    }

    {
        // test const_iterator postfix++
        unsigned int nownum = 0;

        for (btree_type::const_iterator i = set.begin();
             i != set.end(); i++)
        {
            die_unless(nownum++ == *i);
        }

        die_unless(nownum == maxnum);
    }

    {
        // test const_iterator postfix--
        unsigned int nownum = maxnum;

        btree_type::const_iterator i;
        for (i = --set.end(); i != set.begin(); i--)
        {
            die_unless(--nownum == *i);
        }

        die_unless(--nownum == *i);

        die_unless(nownum == 0);
    }

    {
        // test reverse_iterator postfix++
        unsigned int nownum = maxnum;

        for (btree_type::reverse_iterator i = set.rbegin();
             i != set.rend(); i++)
        {
            die_unless(--nownum == *i);
        }

        die_unless(nownum == 0);
    }

    {
        // test reverse_iterator postfix--
        unsigned int nownum = 0;

        btree_type::reverse_iterator i;
        for (i = --set.rend(); i != set.rbegin(); i--)
        {
            die_unless(nownum++ == *i);
        }

        die_unless(nownum++ == *i);

        die_unless(nownum == maxnum);
    }

    {
        // test const_reverse_iterator postfix++
        unsigned int nownum = maxnum;

        for (btree_type::const_reverse_iterator i = set.rbegin();
             i != set.rend(); i++)
        {
            die_unless(--nownum == *i);
        }

        die_unless(nownum == 0);
    }

    {
        // test const_reverse_iterator postfix--
        unsigned int nownum = 0;

        btree_type::const_reverse_iterator i;
        for (i = --set.rend(); i != set.rbegin(); i--)
        {
            die_unless(nownum++ == *i);
        }

        die_unless(nownum++ == *i);

        die_unless(nownum == maxnum);
    }
}

void test_iterator5() {
    typedef tlx::btree_set<
            unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    btree_type set;

    unsigned int maxnum = 100;

    for (unsigned int i = 0; i < maxnum; ++i)
    {
        set.insert(i);
    }

    {
        btree_type::iterator it;

        it = set.begin();
        it--;
        die_unless(it == set.begin());

        it = set.begin();
        --it;
        die_unless(it == set.begin());

        it = set.end();
        it++;
        die_unless(it == set.end());

        it = set.end();
        ++it;
        die_unless(it == set.end());
    }

    {
        btree_type::const_iterator it;

        it = set.begin();
        it--;
        die_unless(it == set.begin());

        it = set.begin();
        --it;
        die_unless(it == set.begin());

        it = set.end();
        it++;
        die_unless(it == set.end());

        it = set.end();
        ++it;
        die_unless(it == set.end());
    }

    {
        btree_type::reverse_iterator it;

        it = set.rbegin();
        it--;
        die_unless(it == set.rbegin());

        it = set.rbegin();
        --it;
        die_unless(it == set.rbegin());

        it = set.rend();
        it++;
        die_unless(it == set.rend());

        it = set.rend();
        ++it;
        die_unless(it == set.rend());
    }

    {
        btree_type::const_reverse_iterator it;

        it = set.rbegin();
        it--;
        die_unless(it == set.rbegin());

        it = set.rbegin();
        --it;
        die_unless(it == set.rbegin());

        it = set.rend();
        it++;
        die_unless(it == set.rend());

        it = set.rend();
        ++it;
        die_unless(it == set.rend());
    }
}

// TODO test_multi
void test_erase_iterator1() {
    if (!test_multi) return;

    typedef tlx::btree_multimap<
            int, int,
            std::less<int>, traits_nodebug<int> > btree_type;

    btree_type map;

    const int size1 = 32;
    const int size2 = 256;

    for (int i = 0; i < size1; ++i)
    {
        for (int j = 0; j < size2; ++j)
        {
            map.insert2(i, j);
        }
    }

    die_unless(map.size() == size1 * size2);

    // erase in reverse order. that should be the worst case for
    // erase_iter()

    for (int i = size1 - 1; i >= 0; --i)
    {
        for (int j = size2 - 1; j >= 0; --j)
        {
            // find iterator
            btree_type::iterator it = map.find(i);

            while (it != map.end() && it->first == i && it->second != j)
                ++it;

            die_unless(it->first == i);
            die_unless(it->second == j);

            size_t mapsize = map.size();
            map.erase(it);
            die_unless(map.size() == mapsize - 1);
        }
    }

    die_unless(map.size() == 0);
}

void test_iterators() {
    test_iterator1();
    test_iterator2();
    test_iterator3();
    test_iterator4();
    test_iterator5();
    test_erase_iterator1();
}

/******************************************************************************/
// Test with Structs

struct TestData {
    unsigned int a, b;

    // required by the btree
    TestData()
        : a(0), b(0)
    { }

    // also used as implicit conversion constructor
    inline TestData(unsigned int _a)
        : a(_a), b(0)
    { }
};

std::ostream& operator<<(std::ostream& os, const TestData& d)
{
    os << '(' << d.a << ',' << d.b << ')';
    return os;
}

struct TestCompare {
    unsigned int somevalue;

    inline TestCompare(unsigned int sv)
        : somevalue(sv)
    { }

    bool operator () (const struct TestData& a, const struct TestData& b) const {
        return a.a > b.a;
    }
};

void test_struct() {
    if (test_multi) {
        typedef tlx::btree_multiset<struct TestData, struct TestCompare,
                                    struct traits_nodebug<struct TestData> > btree_type;

        btree_type bt(TestCompare(42));

        srand(seed);
        for (unsigned int i = 0; i < 320; i++)
        {
            die_unless(bt.size() == i);
            bt.insert(rand() % 100);
            die_unless(bt.size() == i + 1);
        }

        srand(seed);
        for (unsigned int i = 0; i < 320; i++)
        {
            die_unless(bt.size() == 320 - i);
            die_unless(bt.erase_one(rand() % 100));
            die_unless(bt.size() == 320 - i - 1);
        }
    } else {
        // from frozenca's btree tests
        typedef tlx::btree_set<struct TestData, struct TestCompare,
                                    struct traits_nodebug<struct TestData> >
        btree_type;

        btree_type btree(TestCompare(42));

        int n = 320;
        std::mt19937 gen(seed);
        std::vector<int> v(n);

        std::iota(v.begin(), v.end(), 0);
        unsigned long size = 0;

        // random insert
        std::ranges::shuffle(v, gen);
        for (auto num : v) {
            die_unless(btree.insert(num).second);
            die_unless(btree.size() == ++size);
        }

        btree.verify();

        // random lookup
        std::ranges::shuffle(v, gen);
        for (auto num : v) {
          die_unless(btree.exists(num));
        }

        // random erase
        std::ranges::shuffle(v, gen);
        for (auto num : v) {
            bool res = btree.erase(num);
            die_unless(res);
            die_unless(btree.size() == --size);
        }
    }
}

/******************************************************************************/
// Test Relations

void test_relations() {
#if test_multi
    typedef tlx::btree_multiset<
            unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;
#else
    typedef tlx::btree_set<
        unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;
#endif

    btree_type bt1, bt2;

    if (test_multi) {
        srand(seed);
        for (unsigned int i = 0; i < 320; i++)
        {
            unsigned int key = rand() % 1000;

            bt1.insert(key);
            bt2.insert(key);
        }
    } else {
        // from frozenca's btree tests
        btree_type btree;
        int n = 320;

        std::mt19937 gen(seed);
        std::vector<int> v(n);
        std::iota(v.begin(), v.end(), 0);
        std::ranges::shuffle(v, gen);

        for (auto num : v) {
            bt1.insert(num);
            bt2.insert(num);
        }
    }


    die_unless(bt1 == bt2);

    bt1.insert(499);
    bt2.insert(500);

    die_unless(bt1 != bt2);
    die_unless(bt1 < bt2);
    die_unless(!(bt1 > bt2));

    bt1.insert(500);
    bt2.insert(499);

    die_unless(bt1 == bt2);
    die_unless(bt1 <= bt2);

    // test assignment operator
    btree_type bt3;

    bt3 = bt1;
    die_unless(bt1 == bt3);
    die_unless(bt1 >= bt3);

    // test copy constructor
    btree_type bt4 = bt3;

    die_unless(bt1 == bt4);
}

/******************************************************************************/
// Test Bulk Load

void test_bulkload_set_instance(size_t numkeys, unsigned int mod) {
    // TODO this isn't very important rn but implement later please
    if (!test_multi) return;

    typedef tlx::btree_multiset<
            unsigned int,
            std::less<unsigned int>, traits_nodebug<unsigned int> > btree_type;

    std::vector<unsigned int> keys(numkeys);

    srand(seed);
    for (unsigned int i = 0; i < numkeys; i++)
    {
        keys[i] = rand() % mod;
    }

    std::sort(keys.begin(), keys.end());

    btree_type bt;
    bt.bulk_load(keys.begin(), keys.end());

    unsigned int i = 0;
    for (btree_type::iterator it = bt.begin();
         it != bt.end(); ++it, ++i)
    {
        die_unless(*it == keys[i]);
    }
}

void test_bulkload_map_instance(size_t numkeys, unsigned int mod) {
    // TODO see above todo
    if (!test_multi) return;

    typedef tlx::btree_multimap<
            int, std::string,
            std::less<int>, traits_nodebug<int> > btree_type;

    std::vector<std::pair<int, std::string> > pairs(numkeys);

    srand(seed);
    for (unsigned int i = 0; i < numkeys; i++)
    {
        pairs[i].first = rand() % mod;
        pairs[i].second = "key";
    }

    std::sort(pairs.begin(), pairs.end());

    btree_type bt;
    bt.bulk_load(pairs.begin(), pairs.end());

    unsigned int i = 0;
    for (btree_type::iterator it = bt.begin();
         it != bt.end(); ++it, ++i)
    {
        die_unless(*it == pairs[i]);
    }
}

void test_bulkload() {
    for (size_t n = 6; n < 3200; ++n)
        test_bulkload_set_instance(n, 1000);

    test_bulkload_set_instance(31996, 10000);
    test_bulkload_set_instance(32000, 10000);
    test_bulkload_set_instance(117649, 100000);

    for (size_t n = 6; n < 3200; ++n)
        test_bulkload_map_instance(n, 1000);

    test_bulkload_map_instance(31996, 10000);
    test_bulkload_map_instance(32000, 10000);
    test_bulkload_map_instance(117649, 100000);
}

/******************************************************************************/
// Test Multithreading
typedef tlx::btree_set<
        unsigned int,
        std::less<unsigned int>, traits_nodebug<unsigned int> > set_type;

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

// Global array of thread information
std::vector<thread_info> global_thread_info(NUM_THREADS);
std::atomic<int> thread_count(0);
std::map<std::thread::id, int> thread_id_map;

void record_lock(void* node, int lock_type) {
    if (cur_numthreads > 1 && local_debug_info.tinfo) {
        local_debug_info.tinfo->cur_node = node;
        local_debug_info.tinfo->op = lock_type;

        std::lock_guard<std::mutex> l(printmtx);
        set_type::btree_impl::node *nodep = static_cast<set_type::btree_impl::node *>(node);
        std::cout << format_current_time() << " thread " <<
          local_debug_info.tinfo->threadidx << " node "
                  << node << " (r" << nodep->lock->numreader
                  << "|w" << nodep->lock->haswriter
                  << " waiter:r" << nodep->lock->readerswaiting
                  << "|w" << nodep->lock->writerswaiting
                  << "|u" << nodep->lock->upgradewaiting
                  << ") "
                  << lock_type_to_string(lock_type)
                << " " << stack_sym() << std::endl;
    }
}

void print_threads_states(void)
{
    my_multi_thread_set.print(std::cout);
    for (int i = 0; i < cur_numthreads; ++i) {
        std::cout << "Thread " << i + thread_start_idx << " id: " << global_thread_info[i].id
            << " - Node: " << global_thread_info[i].cur_node
            << ", Operation: " << lock_type_to_string(global_thread_info[i].op)
            << std::endl;
        if (global_thread_info[i].cur_node) {
            set_type::btree_impl::node *nodep = static_cast<set_type::btree_impl::node *>(global_thread_info[i].cur_node);
            auto lock = nodep->lock;
            std::cout << "  curread: ";
            for (auto id: lock->curread) {
                std::cout << thread_id_map[id] << ' ';
            }
            std::cout << std::endl;
            std::cout << "  curwrite: ";
            for (auto id: lock->curwrite) {
                std::cout << thread_id_map[id] << ' ';
            }
            std::cout << std::endl;
        }
    }
}
// Signal handler for SIGUSR1
void signal_handler(int signum) {
    if (signum == SIGUSR1) {
        std::cout << "Received SIGUSR1. Current thread states:\n";
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

void before_assert(void)
{
    static bool tree_printed = false;
    if (!tree_printed) { // only print once
        tree_printed = true;
        std::lock_guard<std::mutex> l(printmtx);
        std::cout << "======= print thread state before assert =======\n";
        print_threads_states();
        std::cout << std::endl;
        return;
    } else {
        sleep(3600);
    }
}

void print(const char* op, int val, int id) {
    std::lock_guard<std::mutex> l(printmtx);
    std::cout << seqnum++ << ": thread " << id << " doing " << op
        << " value: " << val << std::endl;
}

void thread_func(set_type& my_set, int insert_prob, int lookup_prob, int delete_prob, int id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, 99);
    std::uniform_int_distribution<> key_dist(0, MAX_KEY - 1);

    initialize_thread_info(id);
    //usleep(10 * 1000 * 1000ull); // sleep for debugging

    for (int i = 0; i < NUM_OPERATIONS; ++i) {
        int key = key_dist(gen);
        int operation = dist(gen);

        if (operation < insert_prob)
        {
            std::lock_guard<std::mutex> lock(truth_source[key].mtx);
            print("insert", key, id);
            bool succeeded = my_set.insert(key).second;
            die_unless(succeeded != truth_source[key].in_set);
            truth_source[key].in_set = true;
        }
        else if (operation < insert_prob + lookup_prob)
        {
            std::lock_guard<std::mutex> lock(truth_source[key].mtx);
            print("find", key, id);
            // using exists because this currently doesn't support iterators
            bool found = my_set.exists(key);
            die_unless(found == truth_source[key].in_set);
        }
        else if (operation < insert_prob + lookup_prob + delete_prob)
        {
            std::lock_guard<std::mutex> lock(truth_source[key].mtx);
            print("erase", key, id);
            bool erased = my_set.erase(key);
            die_unless(erased == truth_source[key].in_set);
            truth_source[key].in_set = false;
        }
        //usleep(10 * 1000 * 1000ull); // sleep for debugging
    }

    cleanup_thread_info();
}

void test_multithread() {
    // Probability out of 100
    int insert_prob = 40;
    int lookup_prob = 40;
    int delete_prob = 20;

    // Register signal handler for SIGUSR1
    std::signal(SIGUSR1, signal_handler);

    while (cur_numthreads <= NUM_THREADS) {
        std::vector<std::thread> threads;
        for (int i = 0; i < cur_numthreads; ++i) {
            threads.emplace_back(thread_func, std::ref(my_multi_thread_set), insert_prob, lookup_prob, delete_prob, i);
        }

        for (auto& th : threads) {
            th.join();
        }

        cur_numthreads++;
    }
}

int main() {
    std::cout << "seed: " << seed << std::endl;
    std::cout << "pid: " << getpid() << std::endl;
    test_simple();
    if (tlx_more_tests) {
        test_large();
        // TODO test_large_sequence();
        test_bounds();
        test_iterators();
        test_struct();
        test_relations();
        test_bulkload();
    }
    if (multithread) {
        test_multithread();
    }
    std::cout << "test successful!" << std::endl;
    return 0;
}

/******************************************************************************/
