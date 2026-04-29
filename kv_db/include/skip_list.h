#pragma once

#include <iostream>
#include <vector>
#include <shared_mutex>
#include <mutex>
#include <random>
#include "arena.h"
#include "lru_cache.h"

template<typename K, typename V>
class Node {
public:
    K key;
    V value;
    Node** forward;

    Node(K k, V v, int level, Arena* arena) : key(k), value(v) {
        // Allocate array of pointers using Arena
        forward = reinterpret_cast<Node**>(arena->AllocateAligned(sizeof(Node*) * level));
        for (int i = 0; i < level; ++i) {
            forward[i] = nullptr;
        }
    }
};

template<typename K, typename V>
class SkipList {
public:
    SkipList(int max_level = 32, float p = 0.25, size_t cache_capacity = 10000)
        : max_level_(max_level), p_(p), level_(1), cache_(cache_capacity) {
        head_ = NewNode(K(), V(), max_level_);
    }

    ~SkipList() {
        // No need to manually delete nodes, Arena will clean up all memory
    }

    bool Insert(K key, V value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        Node<K, V>* current = head_;
        std::vector<Node<K, V>*> update(max_level_, nullptr);

        for (int i = level_ - 1; i >= 0; i--) {
            while (current->forward[i] && current->forward[i]->key < key) {
                current = current->forward[i];
            }
            update[i] = current;
        }

        current = current->forward[0];

        if (current && current->key == key) {
            current->value = value;
            cache_.Put(key, value); // Update cache on write
            return false; // updated
        }

        int new_level = RandomLevel();
        if (new_level > level_) {
            for (int i = level_; i < new_level; i++) {
                update[i] = head_;
            }
            level_ = new_level;
        }

        Node<K, V>* new_node = NewNode(key, value, new_level);
        for (int i = 0; i < new_level; i++) {
            new_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = new_node;
        }

        cache_.Put(key, value); // Insert into cache
        return true;
    }

    bool Search(K key, V& value) {
        // First check LRU cache (O(1) and lock-free relative to SkipList)
        if (cache_.Get(key, value)) {
            return true; // Cache hit
        }

        // Cache miss, search in SkipList
        std::shared_lock<std::shared_mutex> lock(mutex_);
        Node<K, V>* current = head_;

        for (int i = level_ - 1; i >= 0; i--) {
            while (current->forward[i] && current->forward[i]->key < key) {
                current = current->forward[i];
            }
        }

        current = current->forward[0];

        if (current && current->key == key) {
            value = current->value;
            // Write back to cache
            cache_.Put(key, value);
            return true;
        }

        return false;
    }

    bool Delete(K key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        Node<K, V>* current = head_;
        std::vector<Node<K, V>*> update(max_level_, nullptr);

        for (int i = level_ - 1; i >= 0; i--) {
            while (current->forward[i] && current->forward[i]->key < key) {
                current = current->forward[i];
            }
            update[i] = current;
        }

        current = current->forward[0];

        if (current && current->key == key) {
            for (int i = 0; i < level_; i++) {
                if (update[i]->forward[i] != current) {
                    break;
                }
                update[i]->forward[i] = current->forward[i];
            }

            while (level_ > 1 && head_->forward[level_ - 1] == nullptr) {
                level_--;
            }
            
            // Remove from cache
            cache_.Erase(key);
            
            // Note: We don't free the node memory here; it's managed by Arena.
            return true;
        }

        return false;
    }

private:
    Node<K, V>* NewNode(K key, V value, int level) {
        char* mem = arena_.AllocateAligned(sizeof(Node<K, V>));
        return new (mem) Node<K, V>(key, value, level, &arena_);
    }

    int RandomLevel() {
        int level = 1;
        while (static_cast<float>(rand()) / RAND_MAX < p_ && level < max_level_) {
            level++;
        }
        return level;
    }

    int max_level_;
    float p_;
    int level_;
    Node<K, V>* head_;
    std::shared_mutex mutex_;
    Arena arena_;
    ConcurrentLRUCache<K, V> cache_;
};
