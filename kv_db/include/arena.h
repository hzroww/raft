#pragma once

#include <vector>
#include <cstddef>
#include <atomic>

// Bump allocator for objects with the same lifetime. Not thread-safe; the caller
// must provide synchronization if one Arena is shared across threads.
class Arena {
public:
    Arena();
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;
    Arena(Arena&&) = delete;
    Arena& operator=(Arena&&) = delete;

    ~Arena();

    char* Allocate(size_t bytes);
    char* AllocateAligned(size_t bytes);
    size_t MemoryUsage() const;

private:
    char* AllocateFallback(size_t bytes);
    char* AllocateNewBlock(size_t block_bytes);

    char* alloc_ptr_;
    size_t alloc_bytes_remaining_;
    std::vector<char*> blocks_;
    std::atomic<size_t> memory_usage_;

    static constexpr int kBlockSize = 4096;
};
