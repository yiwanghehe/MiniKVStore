package com.yw.pool;

import com.yw.node.Node;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class NodePool <K extends Comparable <K>, V> {
    private final ConcurrentLinkedQueue<Node<K, V>> pool = new ConcurrentLinkedQueue<>();
    private final AtomicInteger poolSize = new AtomicInteger(0); // 原子计数器来追踪池的大小
    private static final int MAX_POOL_SIZE = 100000; // 设置池上限，防止内存无限增长

    /**
     * 从池中获取一个节点。如果池为空，则创建一个新节点。
     *
     * @param key   节点的键
     * @param value 节点的值
     * @param level 节点的层级
     * @return 一个初始化好的节点
     */
    public Node<K, V> acquire(K key, V value, int level) {
        Node<K, V> node = pool.poll();
        if (node != null) {
            // 如果从池中获取到节点，则重置其状态
            poolSize.decrementAndGet(); // 池中出队节点，计数器-1
            node.reset(key, value, level);
            return node;
        }
        // 池为空，创建一个新节点
        return new Node<>(key, value, level);
    }

    /**
     * 将一个不再使用的节点返回到池中。
     *
     * @param node 要释放的节点
     */
    public void release(Node<K, V> node) {
        // 不使用ConcurrentLinkedQueue里的size()来精确判断对象池大小，内置size()性能极差，时间复杂度O(N)
        // 自身维护一个高效的原子计数器来进行检查
        if (poolSize.get() < MAX_POOL_SIZE) {
            pool.offer(node);
            poolSize.incrementAndGet(); // 放入池中，计数器+1
        }
        // 如果池已满，则不添加，让GC回收该节点
    }
}
