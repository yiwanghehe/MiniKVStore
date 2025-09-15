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

    /**
     * 重置节点状态以供对象池复用。
     * @param key 新的键
     * @param value 新的值
     * @param level 新的层级
     */
    public void reset(K key, V value, int level) {
        this.key = key;
        this.value = value;
        this.level = level;

        // 调整forwards的大小以匹配新的层级
        int requiredSize = level + 1;
        while (this.forwards.size() < requiredSize) {
            this.forwards.add(null);
        }
        while (this.forwards.size() > requiredSize) {
            this.forwards.remove(this.forwards.size() - 1);
        }

        // 确保所有指针都被清空
        for (int i = 0; i < requiredSize; i++) {
            this.forwards.set(i, null);
        }
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
