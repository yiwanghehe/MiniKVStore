package com.yw.pool;

import com.yw.node.Node;

import java.util.concurrent.ConcurrentLinkedQueue;

public class NodePool <K extends Comparable <K>, V> {
    private final ConcurrentLinkedQueue<Node<K, V>> pool = new ConcurrentLinkedQueue<>();
    private static final int MAX_POOL_SIZE = 1000000; // 设置池上限，防止内存无限增长

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
            node.reset(key, value, level);
//            System.out.println("从对象池中创建对象");
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
        if (pool.size() < MAX_POOL_SIZE) {
            pool.offer(node);
//            System.out.println("归还节点");
        }
        // 如果池已满，则不添加，让GC回收该节点
    }
}
