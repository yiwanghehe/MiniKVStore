package com.yw.node;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Node用来存储数据的节点
 *
 * @param <K>
 * @param <V>
 */
public class Node<K extends Comparable<K>, V> {
    private K key;
    private V value;
    private Integer level;
    public ArrayList<Node<K, V>> forwards;

    public Node(K key, V value, Integer level) {
        this.key = key;
        this.value = value;
        this.level = level;
        this.forwards = new ArrayList<>(Collections.nCopies(level + 1, null));
    }

    public K getKey() {
        return this.key;
    }

    public V getValue() {
        return this.value;
    }

    public Integer getLevel() {
        return this.level;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public ArrayList<Node<K, V>> getForwards() {
        return this.forwards;
    }

}
