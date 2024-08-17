#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>

#define VTX_BTREE_CONCUR_TEST

#include <tlx/container/btree_set.hpp>
#include <tlx/container/btree_map.hpp>

#include <set>

#include <tlx/die.hpp>
#include <tlx/timestamp.hpp>

// *** Settings

int cur_numthreads = -1; // not used
const bool debug_print = false;

//! starting number of items to insert
const size_t min_items = 125;

//! maximum number of items to insert
const size_t max_items = 1024000 * 64;

//! number of threads operating at a time
const int num_threads = 1;

//! random seed
const int seed = 34234235; //std::random_device{}();

//! Traits used for the speed tests, BTREE_DEBUG is not defined.
template <int InnerSlots, int LeafSlots>
struct btree_traits_speed : tlx::btree_default_traits<size_t, size_t> {
    static const bool self_verify = false;
    static const bool debug = false;

    static const int leaf_slots = InnerSlots; // TODO why are these swapped?
    static const int inner_slots = LeafSlots;

    static const size_t binsearch_threshold = 256 * 1024 * 1024; // never
};

// -----------------------------------------------------------------------------

//! Test a generic set type with insertions
template <typename SetType>
class Test_Set_Insert
{
public:
    Test_Set_Insert(size_t) { }

    static const char * op() { return "set_insert"; }

private:
    SetType set;
    std::vector<int> order;

    void thread_func(int lower, int upper) {
        for (int i = lower; i < upper; ++i) {
            set.insert(order[i]);
        }
    }

public:
    void run(size_t items) {
        std::mt19937 gen(seed);

        order.resize(items);
        std::iota(order.begin(), order.end(), 0);
        std::ranges::shuffle(order, gen);

        std::vector<std::thread> threads;

        int per_thread = items / num_threads;

        int cur = 0;
        for (int i = 0; i < num_threads - 1; ++i) {
            int newcur = cur + per_thread;
            threads.emplace_back(&Test_Set_Insert::thread_func, this, cur, newcur);
            cur = newcur;
        }
        threads.emplace_back(&Test_Set_Insert::thread_func, this, cur, items);


        for (auto& t : threads) t.join();

        die_unless(set.size() == items);
    }
};

//! Test a generic set type with insert, find and delete sequences
template <typename SetType>
class Test_Set_InsertFindDelete
{
public:
    Test_Set_InsertFindDelete(size_t) { }

    static const char * op() { return "set_insert_find_delete"; }
private:
    SetType set;
    std::vector<int> order;

    void insert(int lower, int upper) {
        for (int i = lower; i < upper; ++i) {
            set.insert(order[i]);
        }
    }

    void find(int lower, int upper) {
        for (int i = lower; i < upper; ++i) {
            set.find(order[i]); // TODO will this actually happen
        }
    }

    void erase(int lower, int upper) {
        for (int i = lower; i < upper; ++i) {
            set.erase(order[i]);
        }
    }
public:
    void run(size_t items) {
        std::mt19937 gen(seed);

        order.resize(items);
        std::iota(order.begin(), order.end(), 0);
        std::ranges::shuffle(order, gen);

        std::vector<std::thread> threads;

        int per_thread = items / num_threads;

        int cur = 0;
        for (int i = 0; i < num_threads - 1; ++i) {
            int newcur = cur + per_thread;
            threads.emplace_back(&Test_Set_InsertFindDelete::insert,
                    this, cur, newcur);
            cur = newcur;
        }
        threads.emplace_back(&Test_Set_InsertFindDelete::insert,
                this, cur, items);

        for (auto& t : threads) t.join();

        die_unless(set.size() == items);

        threads.resize(0);
        std::ranges::shuffle(order, gen);
        for (int i = 0; i < num_threads - 1; ++i) {
            int newcur = cur + per_thread;
            threads.emplace_back(&Test_Set_InsertFindDelete::find,
                    this, cur, newcur);
            cur = newcur;
        }
        threads.emplace_back(&Test_Set_InsertFindDelete::find,
                this, cur, items);
        for (auto& t : threads) t.join();

        threads.resize(0);
        std::ranges::shuffle(order, gen);
        for (int i = 0; i < num_threads - 1; ++i) {
            int newcur = cur + per_thread;
            threads.emplace_back(&Test_Set_InsertFindDelete::erase,
                    this, cur, newcur);
            cur = newcur;
        }
        threads.emplace_back(&Test_Set_InsertFindDelete::erase,
                this, cur, items);
        for (auto& t : threads) t.join();

        die_unless(set.empty());
    }
};

//! Test a generic set type with insert, find and delete sequences TODO change in actual
template <typename SetType>
class Test_Set_Find
{
public:
    SetType set;

    static const char * op() { return "set_find"; }

    Test_Set_Find(size_t items) {
        std::mt19937 gen(seed);

        order.resize(items);
        std::iota(order.begin(), order.end(), 0);
        std::ranges::shuffle(order, gen);

        for (auto& num : order) {
            set.insert(num);
        }
        die_unless(set.size() == items);
    }

private:
    std::vector<int> order;

    void thread_func(int lower, int upper) {
        for (int i = lower; i < upper; ++i) {
            set.find(order[i]); // TODO will this actually happen
        }
    }

public:
    void run(size_t items) {
        std::vector<std::thread> threads;

        int per_thread = items / num_threads;

        int cur = 0;
        for (int i = 0; i < num_threads - 1; ++i) {
            int newcur = cur + per_thread;
            threads.emplace_back(&Test_Set_Find::thread_func, this, cur, newcur);
            cur = newcur;
        }
        threads.emplace_back(&Test_Set_Find::thread_func, this, cur, items);


        for (auto& t : threads) t.join();
    }
};

//! Construct different set types for a generic test class
template <template <typename SetType> class TestClass>
struct TestFactory_Set {

    //! Test the std::set
    typedef TestClass<std::set<size_t> > StdSet;

    //! Test the B+ tree with a specific leaf/inner slots
    template <int Slots>
    struct BtreeSet
        : TestClass<tlx::btree_set<
                        size_t, std::less<size_t>,
                        struct btree_traits_speed<Slots, Slots> > > {
        BtreeSet(size_t n)
            : TestClass<tlx::btree_set<
                            size_t, std::less<size_t>,
                            struct btree_traits_speed<Slots, Slots> > >(n) { }
    };

    //! Run tests on all set types
    void call_testrunner(size_t items);
};

// -----------------------------------------------------------------------------

//! Test a generic map type with insertions
template <typename MapType>
class Test_Map_Insert
{
public:
    Test_Map_Insert(size_t) { }

    static const char * op() { return "map_insert"; }

    void run(size_t items) {
        MapType map;

        std::default_random_engine rng(seed);
        for (size_t i = 0; i < items; i++) {
            size_t r = rng();
            map.insert(std::make_pair(r, r));
        }

        die_unless(map.size() == items);
    }
};

//! Test a generic map type with insert, find and delete sequences
template <typename MapType>
class Test_Map_InsertFindDelete
{
public:
    Test_Map_InsertFindDelete(size_t) { }

    static const char * op() { return "map_insert_find_delete"; }

    void run(size_t items) {
        MapType map;

        std::default_random_engine rng(seed);
        for (size_t i = 0; i < items; i++) {
            size_t r = rng();
            map.insert(std::make_pair(r, r));
        }

        die_unless(map.size() == items);

        rng.seed(seed);
        for (size_t i = 0; i < items; i++)
            map.find(rng());

        rng.seed(seed);
        for (size_t i = 0; i < items; i++)
            map.erase(map.find(rng()));

        die_unless(map.empty());
    }
};

//! Test a generic map type with insert, find and delete sequences
template <typename MapType>
class Test_Map_Find
{
public:
    MapType map;

    static const char * op() { return "map_find"; }

    Test_Map_Find(size_t items) {
        std::default_random_engine rng(seed);
        for (size_t i = 0; i < items; i++) {
            size_t r = rng();
            map.insert(std::make_pair(r, r));
        }

        die_unless(map.size() == items);
    }

    void run(size_t items) {
        std::default_random_engine rng(seed);
        for (size_t i = 0; i < items; i++)
            map.find(rng());
    }
};

//! Construct different map types for a generic test class
template <template <typename MapType> class TestClass>
struct TestFactory_Map {
    //! Test the B+ tree with a specific leaf/inner slots
    template <int Slots>
    struct BtreeMap
        : TestClass<tlx::btree_map<
                        size_t, size_t, std::less<size_t>,
                        struct btree_traits_speed<Slots, Slots> > > {
        BtreeMap(size_t n)
            : TestClass<tlx::btree_map<
                            size_t, size_t, std::less<size_t>,
                            struct btree_traits_speed<Slots, Slots> > >(n) { }
    };

    //! Run tests on all map types
    void call_testrunner(size_t items);
};

// -----------------------------------------------------------------------------

size_t repeat_until;

//! Repeat (short) tests until enough time elapsed and divide by the repeat.
template <typename TestClass>
void testrunner_loop(size_t items, const std::string& container_name) {

    size_t repeat = 0;
    double ts1, ts2;

    do
    {
        // count repetition of timed tests
        repeat = 0;

        {
            // initialize test structures
            TestClass test(items);

            ts1 = tlx::timestamp();

            for (size_t r = 0; r <= repeat_until; r += items)
            {
                // run timed test procedure
                test.run(items);
                ++repeat;
            }

            ts2 = tlx::timestamp();
        }

        std::cout << "Insert " << items << " repeat " << (repeat_until / items)
                  << " time " << (ts2 - ts1) << "\n";

        // discard and repeat if test took less than one second.
        if ((ts2 - ts1) < 1.0) repeat_until *= 2;
    }
    while ((ts2 - ts1) < 1.0);

    std::cout << "RESULT"
              << " container=" << container_name
              << " op=" << TestClass::op()
              << " items=" << items
              << " repeat=" << repeat
              << " time_total=" << (ts2 - ts1)
              << " time="
              << std::fixed << std::setprecision(10) << ((ts2 - ts1) / repeat)
              << " items_per_sec=" << items / (ts2 - ts1)
              << std::endl;
}

// Template magic to emulate a for_each slots. These templates will roll-out
// btree instantiations for each of the Low-High leaf/inner slot numbers.
template <template <int Slots> class Functional, int Low, int High>
struct btree_range {
    void operator () (size_t items, const std::string& container_name) {
        testrunner_loop<Functional<Low> >(
            items, container_name + "<" + std::to_string(Low) + ">"
            " slots=" + std::to_string(Low));
        btree_range<Functional, Low + 2, High>()(items, container_name);
    }
};

template <template <int Slots> class Functional, int Low>
struct btree_range<Functional, Low, Low> {
    void operator () (size_t items, const std::string& container_name) {
        testrunner_loop<Functional<Low> >(
            items, container_name + "<" + std::to_string(Low) + ">"
            " slots=" + std::to_string(Low));
    }
};

template <template <typename Type> class TestClass>
void TestFactory_Set<TestClass>::call_testrunner(size_t items) {
    testrunner_loop<StdSet>(items, "std::set");

#if 0
    btree_range<BtreeSet, min_nodeslots, max_nodeslots>()(
        items, "tlx::btree_set");
#else
    // just pick a few node sizes for quicker tests
    testrunner_loop<BtreeSet<4> >(items, "tlx::btree_set<4> slots=4");
    testrunner_loop<BtreeSet<8> >(items, "tlx::btree_set<8> slots=8");
    testrunner_loop<BtreeSet<16> >(items, "tlx::btree_set<16> slots=16");
    testrunner_loop<BtreeSet<32> >(items, "tlx::btree_set<32> slots=32");
    testrunner_loop<BtreeSet<64> >(items, "tlx::btree_set<64> slots=64");
    testrunner_loop<BtreeSet<128> >(
        items, "tlx::btree_set<128> slots=128");
    testrunner_loop<BtreeSet<256> >(
        items, "tlx::btree_set<256> slots=256");
#endif
}

template <template <typename Type> class TestClass>
void TestFactory_Map<TestClass>::call_testrunner(size_t items) {
#if 0
    btree_range<BtreeMap, min_nodeslots, max_nodeslots>()(
        items, "tlx::btree_multimap");
#else
    // just pick a few node sizes for quicker tests
    testrunner_loop<BtreeMap<4> >(items, "tlx::btree_multimap<4> slots=4");
    testrunner_loop<BtreeMap<8> >(items, "tlx::btree_multimap<8> slots=8");
    testrunner_loop<BtreeMap<16> >(items, "tlx::btree_multimap<16> slots=16");
    testrunner_loop<BtreeMap<32> >(items, "tlx::btree_multimap<32> slots=32");
    testrunner_loop<BtreeMap<64> >(items, "tlx::btree_multimap<64> slots=64");
    testrunner_loop<BtreeMap<128> >(
        items, "tlx::btree_multimap<128> slots=128");
    testrunner_loop<BtreeMap<256> >(
        items, "tlx::btree_multimap<256> slots=256");
#endif
}

//! Speed test them!
int main() {
    {   // Set - speed test only insertion

        repeat_until = min_items;

        for (size_t items = min_items; items <= max_items; items *= 2)
        {
            std::cout << "set: insert " << items << "\n";
            TestFactory_Set<Test_Set_Insert>().call_testrunner(items);
        }
    }

    {   // Set - speed test insert, find and delete

        repeat_until = min_items;

        for (size_t items = min_items; items <= max_items; items *= 2)
        {
            std::cout << "set: insert, find, delete " << items << "\n";
            TestFactory_Set<Test_Set_InsertFindDelete>().call_testrunner(items);
        }
    }

    {   // Set - speed test find only

        repeat_until = min_items;

        for (size_t items = min_items; items <= max_items; items *= 2)
        {
            std::cout << "set: find " << items << "\n";
            TestFactory_Set<Test_Set_Find>().call_testrunner(items);
        }
    }

    /*{   // Map - speed test only insertion

        repeat_until = min_items;

        for (size_t items = min_items; items <= max_items; items *= 2)
        {
            std::cout << "map: insert " << items << "\n";
            TestFactory_Map<Test_Map_Insert>().call_testrunner(items);
        }
    }

    {   // Map - speed test insert, find and delete

        repeat_until = min_items;

        for (size_t items = min_items; items <= max_items; items *= 2)
        {
            std::cout << "map: insert, find, delete " << items << "\n";
            TestFactory_Map<Test_Map_InsertFindDelete>().call_testrunner(items);
        }
    }

    {   // Map - speed test find only

        repeat_until = min_items;

        for (size_t items = min_items; items <= max_items; items *= 2)
        {
            std::cout << "map: find " << items << "\n";
            TestFactory_Map<Test_Map_Find>().call_testrunner(items);
        }
    }*/

    return 0;
}

/******************************************************************************/
