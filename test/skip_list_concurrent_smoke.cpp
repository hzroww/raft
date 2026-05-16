// Smoke test for the concurrent SkipList in kv_db.
//
// Validates Insert/Search/Delete under multi-threaded load, and exercises
// Snapshot() to confirm the resulting node sequence is correctly ordered.
//
// Style follows the other test/*_smoke.cpp executables in this repo: no
// third-party test framework, prints "FAIL: ..." and exits non-zero on
// failure, otherwise prints a PASS line.

#include "skip_list.h"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

void Fail(const std::string& msg) {
    std::cout << "FAIL: " << msg << std::endl;
    std::_Exit(1);
}

void ConcurrentInsertAndSearch() {
    SkipList<int, int> list;

    constexpr int kThreads        = 8;
    constexpr int kPerThreadCount = 2000;

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&list, t]() {
            int base = t * kPerThreadCount;
            for (int i = 0; i < kPerThreadCount; ++i) {
                int key = base + i;
                list.Insert(key, key * 10);
            }
        });
    }
    for (auto& w : workers) w.join();

    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kPerThreadCount; ++i) {
            int key = t * kPerThreadCount + i;
            int value = 0;
            if (!list.Search(key, value)) {
                Fail("missing key after concurrent insert: " + std::to_string(key));
            }
            if (value != key * 10) {
                Fail("wrong value for key=" + std::to_string(key) +
                     " got=" + std::to_string(value));
            }
        }
    }

    auto snap = list.Snapshot();
    if (snap.size() != static_cast<size_t>(kThreads * kPerThreadCount)) {
        Fail("snapshot size mismatch after inserts: got " +
             std::to_string(snap.size()));
    }
    for (size_t i = 1; i < snap.size(); ++i) {
        if (!(snap[i - 1].first < snap[i].first)) {
            Fail("snapshot not strictly ordered at index " + std::to_string(i));
        }
    }
}

void ConcurrentReadWrite() {
    SkipList<int, int> list;

    constexpr int kKeys = 1000;
    for (int k = 0; k < kKeys; ++k) {
        list.Insert(k, 1);
    }

    std::atomic<long long> read_ops{0};
    constexpr int kReaders    = 4;
    constexpr int kReaderPass = 20;
    constexpr int kWriters    = 4;
    constexpr int kWriterPass = 20;
    constexpr int kFinalValue = 9999;

    std::vector<std::thread> readers;
    for (int r = 0; r < kReaders; ++r) {
        readers.emplace_back([&]() {
            int value = 0;
            for (int pass = 0; pass < kReaderPass; ++pass) {
                for (int k = 0; k < kKeys; ++k) {
                    list.Search(k, value);
                    read_ops.fetch_add(1, std::memory_order_relaxed);
                }
                std::this_thread::yield();
            }
        });
    }

    std::vector<std::thread> writers;
    for (int w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w]() {
            for (int pass = 0; pass < kWriterPass; ++pass) {
                for (int k = w; k < kKeys; k += kWriters) {
                    list.Insert(k, pass);
                }
                std::this_thread::yield();
            }
        });
    }

    for (auto& r : readers) r.join();
    for (auto& w : writers) w.join();

    for (int k = 0; k < kKeys; ++k) {
        list.Insert(k, kFinalValue);
    }

    for (int k = 0; k < kKeys; ++k) {
        int value = 0;
        if (!list.Search(k, value)) {
            Fail("key disappeared during read/write: " + std::to_string(k));
        }
        if (value != kFinalValue) {
            Fail("final value mismatch for key=" + std::to_string(k) +
                 " got=" + std::to_string(value));
        }
    }

    std::cout << "info: read/write phase performed "
              << read_ops.load() << " reads" << std::endl;
}

void ConcurrentDelete() {
    SkipList<int, int> list;

    constexpr int kKeys = 4000;
    for (int k = 0; k < kKeys; ++k) {
        list.Insert(k, k * 7);
    }

    constexpr int kThreads = 4;
    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&list, t]() {
            for (int k = 0; k < kKeys; ++k) {
                if ((k % 2) == 0 && (k % kThreads) == t) {
                    list.Delete(k);
                }
            }
        });
    }
    for (auto& w : workers) w.join();

    for (int k = 0; k < kKeys; ++k) {
        int value = 0;
        bool found = list.Search(k, value);
        if ((k % 2) == 0) {
            if (found) {
                Fail("even key still present after delete: " +
                     std::to_string(k));
            }
        } else {
            if (!found) {
                Fail("odd key missing after delete pass: " + std::to_string(k));
            }
            if (value != k * 7) {
                Fail("odd key value corrupted: " + std::to_string(k) +
                     " got=" + std::to_string(value));
            }
        }
    }

    auto snap = list.Snapshot();
    size_t expected = static_cast<size_t>(kKeys / 2);
    if (snap.size() != expected) {
        Fail("snapshot size after delete mismatch: got " +
             std::to_string(snap.size()) +
             " expected " + std::to_string(expected));
    }
    for (size_t i = 0; i < snap.size(); ++i) {
        if ((snap[i].first % 2) == 0) {
            Fail("snapshot still contains even key " +
                 std::to_string(snap[i].first));
        }
        if (i > 0 && !(snap[i - 1].first < snap[i].first)) {
            Fail("snapshot not strictly ordered after delete at index " +
                 std::to_string(i));
        }
    }

    std::cout << "info: snapshot first nodes:";
    for (size_t i = 0; i < snap.size() && i < 8; ++i) {
        std::cout << " (" << snap[i].first << "," << snap[i].second << ")";
    }
    std::cout << std::endl;
}

}  // namespace

int main() {
    std::cout << "stage: ConcurrentInsertAndSearch ..." << std::endl;
    ConcurrentInsertAndSearch();
    std::cout << "stage: ConcurrentReadWrite ..." << std::endl;
    ConcurrentReadWrite();
    std::cout << "stage: ConcurrentDelete ..." << std::endl;
    ConcurrentDelete();
    std::cout << "PASS: skip list concurrent smoke" << std::endl;
    return 0;
}
