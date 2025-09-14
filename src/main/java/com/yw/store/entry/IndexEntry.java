package com.yw.store.entry;

/**
 * 索引块条目
 */
public class IndexEntry implements Comparable<IndexEntry> {
    String lastKey;
    long offset;
    int size;
    public IndexEntry(String lastKey, long offset, int size) {
        this.lastKey = lastKey;
        this.offset = offset;
        this.size = size;
    }

    public String getLastKey() {
        return lastKey;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    @Override
    public int compareTo(IndexEntry o) {
        return this.lastKey.compareTo(o.lastKey);
    }
}
