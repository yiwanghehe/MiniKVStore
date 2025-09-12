package com.yw.skipList;

import com.yw.node.Node;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class SkipList<K extends Comparable<K>, V> {

    /**
     * 读写锁
     */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    /**
     * 跳表的最大层数
     */
    private static final Integer MAX_LEVEL = 32;

    /**
     * 跳表的头节点
     */
    private final Node<K, V> header;

    /**
     * 跳表当前的最大层数. 使用AtomicInteger保证原子性.
     */
    private final AtomicInteger skipListLevel;

    /**
     * 跳表里的节点数. 使用AtomicLong保证原子性.
     */
    private final AtomicLong nodeCount;

    /**
     * 持久化文件保存位置
     */
    private static final String STORE_LOCATION = "./store";

    /**
     * 构造方法
     */
    public SkipList() {
        this.header = new Node<>(null, null, MAX_LEVEL);
        this.skipListLevel = new AtomicInteger(0);
        this.nodeCount = new AtomicLong(0L);
    }

    /**
     * 创建 Node 方法
     *
     * @param key   存入的键
     * @param value 存入的值
     * @param level 该节点所在的层级
     * @return 返回创建后的该节点
     */
    public Node<K, V> createNode(K key, V value, Integer level) {
        return new Node<>(key, value, level);
    }

    /**
     * 生成 Node 所在层级方法
     *
     * @return 返回节点层级
     */
    public Integer generateLevel() {
        int level = 1;
        // 直接使用，无需创建random实例，每个线程有自己的生成器，无竞争，性能最佳
        while (ThreadLocalRandom.current().nextInt(2) == 1) {
            level++;
        }
        return Math.min(level, MAX_LEVEL);
    }

    /**
     * @return 返回跳表中节点的数量
     */
    public Long getNodeCount() {
        return this.nodeCount.get();
    }

    public Node<K, V> getHeader() {
        return this.header;
    }

    /**
     * 向跳表中插入一个键值对，如果跳表中已经存在相同 key 的节点，则更新这个节点的 value
     *
     * @param key   插入的 Node 的键
     * @param value 插入的 Node 的值
     * @return 返回插入结果，插入成功返回 true，插入失败返回 false
     */
    public boolean insert(K key, V value) {
        rwLock.writeLock().lock();
        try {
            Node<K, V> current = this.header; // 指向头节点

            // update用来存储在要插入的节点在各个层中的前节点
            ArrayList<Node<K, V>> update = new ArrayList<>(Collections.nCopies(MAX_LEVEL + 1, null));

            int currentLevel = this.skipListLevel.get();
            for (int i = currentLevel; i >= 0; i--) {
                while (current.getForwards().get(i) != null && current.getForwards().get(i).getKey().compareTo(key) < 0) {
                    current = current.getForwards().get(i);
                }
                update.set(i, current);
            }

            current = current.getForwards().get(0);

            if (current != null && current.getKey().compareTo(key) == 0) {
                current.setValue(value);
                return true;
            }

            Integer newNodeLevel = generateLevel();

            if (current == null || current.getKey().compareTo(key) != 0) {

                if (newNodeLevel > currentLevel) {
                    for (int i = currentLevel + 1; i <= newNodeLevel; i++) {
                        update.set(i, this.header);
                    }
                    this.skipListLevel.set(newNodeLevel);
                }

                Node<K, V> newNode = createNode(key, value, newNodeLevel);
                for (int i = 0; i <= newNodeLevel; i++) {
                    newNode.getForwards().set(i, update.get(i).getForwards().get(i));
                    update.get(i).getForwards().set(i, newNode);
                }
                this.nodeCount.incrementAndGet(); // 使用原子自增
                return true;
            }
            return false;

        } finally {
            rwLock.writeLock().unlock();
        }

    }

    /**
     * 根据 key 删除 SkipList 中的 Node
     *
     * @param key 需要删除的 Node 的 key
     * @return 删除成功返回 true，失败返回 false
     */
    public boolean delete(K key) {
        rwLock.writeLock().lock();
        try {
            Node<K, V> current = this.header;

            ArrayList<Node<K, V>> update = new ArrayList<>(Collections.nCopies(MAX_LEVEL + 1, null));

            int currentLevel = this.skipListLevel.get();
            for (int i = currentLevel; i >= 0; i--) {
                while (current.getForwards().get(i) != null && current.getForwards().get(i).getKey().compareTo(key) < 0) {
                    current = current.getForwards().get(i);
                }
                update.set(i, current);
            }

            current = current.getForwards().get(0);

            if (current != null && current.getKey().compareTo(key) == 0) {
                for (int i = 0; i <= current.getLevel(); i++) {
                    update.get(i).getForwards().set(i, current.getForwards().get(i));
                }

                while (this.skipListLevel.get() > 0 && this.header.getForwards().get(this.skipListLevel.get()) == null) {
                    this.skipListLevel.decrementAndGet(); // 使用原子自减
                }

                this.nodeCount.decrementAndGet(); // 使用原子自减
                return true;
            }

            return false;
        } finally {
            rwLock.writeLock().unlock();
        }

    }

    /**
     * 搜索跳表中是否存在键为 key 的键值对
     *
     * @param key 键
     * @return 跳表中存在键为 key 的键值对返回 true，不存在返回 false
     */
    public boolean search(K key) {
        rwLock.readLock().lock();
        try {
            Node<K, V> current = this.header;

            int currentLevel = this.skipListLevel.get();
            for (int i = currentLevel; i >= 0; i--) {
                while (current.getForwards().get(i) != null && current.getForwards().get(i).getKey().compareTo(key) < 0) {
                    current = current.getForwards().get(i);
                }
            }

            current = current.getForwards().get(0);

            return current != null && current.getKey().compareTo(key) == 0;

        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * 获取键为 key 的 Node 的值
     *
     * @param key 键
     * @return 返回键为 key 的节点的值，如果不存在则返回 null
     */
    public V get(K key) {
        rwLock.readLock().lock();
        try {
            Node<K, V> current = this.header;

            int currentLevel = this.skipListLevel.get();
            for (int i = currentLevel; i >= 0; i--) {
                while (current.getForwards().get(i) != null && current.getForwards().get(i).getKey().compareTo(key) < 0) {
                    current = current.getForwards().get(i);
                }
            }

            current = current.getForwards().get(0);

            if (current != null && current.getKey().compareTo(key) == 0) {
                return current.getValue();
            }
            return null;
        } finally {
            rwLock.readLock().unlock();
        }

    }

    /**
     * 持久化跳表内的数据
     */
    public void dump() {
        rwLock.readLock().lock();
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(STORE_LOCATION))) {
            Node<K, V> current = this.header.getForwards().get(0);
            while (current != null) {
                String data = current.getKey() + ":" + current.getValue() + ";";
                bufferedWriter.write(data);
                bufferedWriter.newLine();
                current = current.getForwards().get(0);
            }
        } catch (IOException e) {
            throw new RuntimeException("持久化失败");
        } finally {
            rwLock.readLock().unlock();
        }
    }

    /**
     * 从文本文件中读取数据
     */
    public void load(Function<String, K> keyDeserializer, Function<String, V> valueDeserializer) {
        try (BufferedReader bufferedReader = new BufferedReader((new FileReader(STORE_LOCATION)))) {
            String data;
            while ((data = bufferedReader.readLine()) != null) {
                Node<K, V> node = getKVFromString(data, keyDeserializer, valueDeserializer);
                if (node != null) {
                    insert(node.getKey(), node.getValue());
                }
            }
            System.out.println("加载成功:" + this.nodeCount.get() + "个数据");
        } catch (IOException e) {
            throw new RuntimeException("加载失败");
        }
    }

    /**
     * 根据文件中的持久化字符串，获取 key 和 value，并将 key 和 value 封装到 Node 对象中
     *
     * @param data              字符串
     * @param keyDeserializer   key反序列化器
     * @param valueDeserializer value反序列化器
     * @return 返回该字符串对应的key和value 组成的 Node 实例，如果字符串非法，则返回 null
     */
    public Node<K, V> getKVFromString(String data, Function<String, K> keyDeserializer, Function<String, V> valueDeserializer) {
        if (!isValidDataString(data)) return null;
        String keyStr = data.substring(0, data.indexOf(":"));
        String valueStr = data.substring(data.indexOf(":") + 1, data.length() - 1);
        K key = keyDeserializer.apply(keyStr);
        V value = valueDeserializer.apply(valueStr);
        return new Node<>(key, value, 0);
    }

    /**
     * 判断读取的data字符串是否合法
     *
     * @param data 字符串
     * @return 合法返回 true，非法返回 false
     */
    public boolean isValidDataString(String data) {
        if (data == null || data.isEmpty()) return false;
        return data.contains(":");
    }

    /**
     * 打印跳表的结构
     */
    public void display() {
        rwLock.readLock().lock();
        try {
            int currentLevel = this.skipListLevel.get();
            for (int i = currentLevel; i >= 0; i--) {
                Node<K, V> current = this.header.getForwards().get(i);
                System.out.print("Level " + i + ": ");
                while (current != null) {
                    System.out.print(current.getKey() + ":" + current.getValue() + ";");
                    current = current.getForwards().get(i);
                }
                System.out.println();
            }
        } finally {
            rwLock.readLock().unlock();
        }
    }
}

