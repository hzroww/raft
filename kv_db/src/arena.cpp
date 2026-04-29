#include "arena.h"

#include <cassert>
#include <cstdint>

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
    for (char* block : blocks_) {
        delete[] block;
    }
}

char* Arena::Allocate(size_t bytes) {
    assert(bytes > 0);
    if (bytes <= alloc_bytes_remaining_) {
        char* result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }
    return AllocateFallback(bytes);
}

char* Arena::AllocateAligned(size_t bytes) {
    const int align = (sizeof(void*) > 8) ? static_cast<int>(sizeof(void*)) : 8;
    static_assert((align & (align - 1)) == 0, "Pointer size should be a power of 2");
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & static_cast<size_t>(align - 1);
    size_t slop = (current_mod == 0 ? 0 : static_cast<size_t>(align) - current_mod);
    size_t needed = bytes + slop;
    char* result;
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & static_cast<size_t>(align - 1)) == 0);
    return result;
}

size_t Arena::MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
}

char* Arena::AllocateFallback(size_t bytes) {
    if (bytes > static_cast<size_t>(kBlockSize / 4)) {
        return AllocateNewBlock(bytes);
    }
    alloc_ptr_ = AllocateNewBlock(static_cast<size_t>(kBlockSize));
    alloc_bytes_remaining_ = static_cast<size_t>(kBlockSize);

    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
    char* result = new char[block_bytes];
    blocks_.push_back(result);
    memory_usage_.fetch_add(block_bytes + sizeof(char*), std::memory_order_relaxed);
    return result;
}
