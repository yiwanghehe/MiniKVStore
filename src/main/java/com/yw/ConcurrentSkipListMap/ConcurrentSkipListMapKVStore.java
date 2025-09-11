package com.yw.ConcurrentSkipListMap;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A Key-Value store implementation using java.util.concurrent.ConcurrentSkipListMap
 * for performance comparison.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class ConcurrentSkipListMapKVStore<K extends Comparable<K>, V> {

    private final ConcurrentSkipListMap<K, V> map;

    public ConcurrentSkipListMapKVStore() {
        this.map = new ConcurrentSkipListMap<>();
    }

    /**
     * Inserts a key-value pair.
     */
    public boolean insert(K key, V value) {
        map.put(key, value);
        return true; // put() always succeeds or updates the value.
    }

    /**
     * Deletes a key-value pair.
     */
    public boolean delete(K key) {
        return map.remove(key) != null;
    }

    /**
     * Searches for a key.
     */
    public boolean search(K key) {
        return map.containsKey(key);
    }

    /**
     * Gets the value for a key.
     */
    public V get(K key) {
        return map.get(key);
    }

    /**
     * Returns the number of elements in the store.
     */
    public long getNodeCount() {
        return map.size();
    }
}

