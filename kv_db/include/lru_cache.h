#pragma once

#include <list>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <memory>
#include <functional>

template<typename K, typename V>
class LRUCacheSegment {
public:
    explicit LRUCacheSegment(size_t capacity) : capacity_(capacity) {}

    bool Get(const K& key, V& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return false;
        }
        // Move to front (most recently used)
        list_.splice(list_.begin(), list_, it->second);
        value = it->second->second;
        return true;
    }

    void Put(const K& key, const V& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second->second = value;
            list_.splice(list_.begin(), list_, it->second);
            return;
        }

        if (list_.size() >= capacity_) {
            auto last = list_.back();
            map_.erase(last.first);
            list_.pop_back();
        }

        list_.emplace_front(key, value);
        map_[key] = list_.begin();
    }

    void Erase(const K& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            list_.erase(it->second);
            map_.erase(it);
        }
    }

private:
    size_t capacity_;
    std::list<std::pair<K, V>> list_;
    std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> map_;
    std::mutex mutex_;
};

template<typename K, typename V, typename Hash = std::hash<K>>
class ConcurrentLRUCache {
public:
    explicit ConcurrentLRUCache(size_t total_capacity, size_t num_segments = 16)
        : num_segments_(num_segments) {
        size_t segment_capacity = total_capacity / num_segments;
        if (segment_capacity == 0) segment_capacity = 1;
        for (size_t i = 0; i < num_segments; ++i) {
            segments_.emplace_back(std::make_unique<LRUCacheSegment<K, V>>(segment_capacity));
        }
    }

    bool Get(const K& key, V& value) {
        return segments_[GetSegmentIndex(key)]->Get(key, value);
    }

    void Put(const K& key, const V& value) {
        segments_[GetSegmentIndex(key)]->Put(key, value);
    }

    void Erase(const K& key) {
        segments_[GetSegmentIndex(key)]->Erase(key);
    }

private:
    size_t GetSegmentIndex(const K& key) const {
        return hash_fn_(key) % num_segments_;
    }

    size_t num_segments_;
    std::vector<std::unique_ptr<LRUCacheSegment<K, V>>> segments_;
    Hash hash_fn_;
};
