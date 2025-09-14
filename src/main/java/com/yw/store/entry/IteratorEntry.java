package com.yw.store.entry;

import com.yw.node.Node;
import com.yw.store.SSTableIterator;

/**
 * 数据块条目
 */
public class IteratorEntry implements Comparable<IteratorEntry> {
    Node<String, String> node;
    SSTableIterator iterator;
    long fileId;

    public IteratorEntry(Node<String, String> node, SSTableIterator iterator, long fileId) {
        this.node = node;
        this.iterator = iterator;
        this.fileId = fileId;
    }

    public Node<String, String> getNode() {
        return node;
    }

    public SSTableIterator getIterator() {
        return iterator;
    }

    public long getFileId() {
        return fileId;
    }

    public void setNode(Node<String, String> node) {
        this.node = node;
    }

    @Override
    public int compareTo(IteratorEntry other){
        int keyCmp = this.node.getKey().compareTo(other.node.getKey());
        if (keyCmp != 0) {
            return keyCmp;
        }
        return Long.compare(other.fileId, this.fileId);
    }
}
