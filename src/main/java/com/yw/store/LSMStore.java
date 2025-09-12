package com.yw.store;

import com.yw.node.Node;
import com.yw.skipList.SkipList;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * LSM-Tree 架构的核心存储引擎。
 * 它负责协调 WAL、MemTable 和 SSTable，对外提供统一的 put/get/delete 接口。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public class LSMStore<K extends Comparable<K>, V> {
    // --- 核心组件 ---
    private volatile SkipList<K, V> activeMemTable; // 当前活跃的可写内存表
    private final WALManager<K, V> walManager; // 预写日志管理器
    private final ConcurrentLinkedQueue<SkipList<K, V>> immutableMemTables; // 只读的、等待刷盘的内存表队列

    // --- 配置与状态 ---
    private static final String DATA_DIR = "./data"; // 数据存储目录
    private static final String SSTABLE_DIR = DATA_DIR + "/sst";
    private static final Long MEMTABLE_THRESHOLD = 10000L; // MemTable切换阈值，简化为条目数
    private final ReentrantLock memtableSwitchLock = new ReentrantLock(); // MemTable切换锁
    private volatile boolean shuttingDown = false; // 关闭的状态标志

    // --- 后台任务 ---
    private final ExecutorService flushExecutor; // 用于执行刷盘任务的单线程执行器

    /**
     * 构造函数，初始化LSM存储引擎。
     *
     * @throws IOException 初始化过程中发生IO错误
     */
    public LSMStore() throws IOException {
        // 确保数据目录存在
        Files.createDirectories(Paths.get(SSTABLE_DIR));

        this.activeMemTable = new SkipList<>();
        this.walManager = new WALManager<>(DATA_DIR);
        this.immutableMemTables = new ConcurrentLinkedQueue<>();

        // 恢复未完成刷盘的MemTable
        recoverFromWAL();

        // 启动后台刷盘线程
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lsm-flush-thread");
            t.setDaemon(true); // 设置为守护线程,不会阻止JVM的退出
            return t;
        });
        this.flushExecutor.submit(this::flushTask);
    }

    /**
     * 向存储引擎中插入或更新一个键值对。
     *
     * @param key   键
     * @param value 值
     * @throws IOException WAL写入失败
     */
    public void put(K key, V value) throws IOException {
        if (shuttingDown) {
            throw new IllegalStateException("存储引擎正在关闭，无法接受新的写入。");
        }

        // 先写入WAL日志，保证数据不丢失
        walManager.logPut(key, value);

        // 写入MemTable
        activeMemTable.insert(key, value);

        // 检查是否切换MemTable
        if (activeMemTable.getNodeCount() >= MEMTABLE_THRESHOLD) {
            switchMemTable();
        }
    }

    /**
     * 切换MemTable，将当前活跃的MemTable变为不可变，并创建一个新的空MemTable。
     */
    private void switchMemTable() {
        memtableSwitchLock.lock();
        try {
            // 防止并发切换
            if (activeMemTable.getNodeCount() == 0) return;

            // 将当前的MemTable放入不可变队列，等待刷盘
            immutableMemTables.add(activeMemTable);
            // 新初始化一个MemTable
            activeMemTable = new SkipList<>();
            // 轮转日志
            walManager.rotateLog();
        } catch (IOException e) {
            System.err.println("切换WAL日志失败: " + e.getMessage());
        } finally {
            memtableSwitchLock.unlock();
        }
    }

    /**
     * 从存储引擎中获取一个键对应的值。
     *
     * @param key 键
     * @return 值，如果不存在则返回null
     * @throws IOException 读取SSTable失败
     */
    public V get(K key) throws IOException {
        V value;

        // 先从活跃的MemTable中查找
        value = activeMemTable.get(key);
        if (value != null) return value;

        // 再从不可变的immutableMemTableQueue中查找(从新到旧)
        for (SkipList<K, V> memTable : immutableMemTables) {
            value = memTable.get(key);
            if (value != null) return value;
        }

        List<Path> sstFiles = Files.list(Paths.get(SSTABLE_DIR))
                .filter(p -> p.toString().endsWith(".sst"))
                .sorted(Comparator.reverseOrder()) // 按文件名倒序，实现从新到旧查找
                .collect(Collectors.toList());

        for (Path sstFile : sstFiles) {
            // (简化实现) 逐行扫描文件查找
            // 生产环境会使用SSTable的索引块来加速查找
            List<String> lines = Files.readAllLines(sstFile);
            for (String line : lines) {
                String[] parts = line.split(":", 2);
                if (parts[0].equals(key.toString())) {
                    // 假设Value是String类型，这里需要更通用的反序列化
                    return (V) parts[1].substring(0, parts[1].length() - 1);
                }
            }
        }

        return null;
    }

    /**
     * 后台刷盘任务，循环执行。
     */
    private void flushTask() {
        while (!shuttingDown || !immutableMemTables.isEmpty()) {
            SkipList<K, V> memTableToFlush = immutableMemTables.poll();
            if (memTableToFlush != null) {
                try {
                    flushMemTableToSSTable(memTableToFlush);
                } catch (IOException e) {
                    System.err.println("刷盘失败: " + e.getMessage());
                    // 实际应用中需要有重试或错误处理机制
                }
            } else {
                // 如果队列为空，且不是在关闭过程中，则休眠
                if (!shuttingDown) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // 如果在休眠时被中断（比如在关闭时），则重新设置中断标志并准备退出
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        System.out.println("后台刷盘任务已退出。");
    }

    /**
     * 将一个MemTable的内容写入一个新的SSTable文件。
     *
     * @param memTable 要刷盘的MemTable
     * @throws IOException 写入失败
     */
    private void flushMemTableToSSTable(SkipList<K, V> memTable) throws IOException {
        long timestamp = System.currentTimeMillis();
        String sstFileName = timestamp + ".sst";
        Path sstFilePath = Paths.get(SSTABLE_DIR, sstFileName);

        System.out.println("开始刷盘: " + memTable.getNodeCount() + " 条记录到 " + sstFileName);

        try (BufferedWriter writer = Files.newBufferedWriter(sstFilePath)) {
            Node<K, V> current = memTable.getHeader().getForwards().get(0);
            while (current != null) {
                writer.write(current.getKey() + ":" + current.getValue() + ";");
                writer.newLine();
                current = current.getForwards().get(0);
            }
        }
        System.out.println("刷盘完成: " + sstFileName);

    }

    /**
     * 从WAL日志中恢复数据。在引擎启动时调用。
     */
    private void recoverFromWAL() throws IOException {
        walManager.recover(activeMemTable);
        System.out.println("从WAL恢复了 " + activeMemTable.getNodeCount() + " 条记录。");
    }

    /**
     * 关闭存储引擎，确保所有数据都已持久化。
     */
    public void close() throws InterruptedException, IOException {
        System.out.println("正在关闭存储引擎...");

        // 设置关闭标志，并拒绝新的写入
        shuttingDown = true;

        memtableSwitchLock.lock();
        try {
            // 将最后的 activeMemTable(如果非空)也加入待刷盘队列
            if (activeMemTable.getNodeCount() > 0) {
                immutableMemTables.add(activeMemTable);
            }
        } finally {
            memtableSwitchLock.unlock();
        }

        // 停止接受新任务，并等待现有任务刷盘完成
        flushExecutor.shutdown();
        if (!flushExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            System.err.println("后台刷盘任务在10秒内未能完成！");
        }

        // 所有内存数据都刷盘后或超时后，才关闭WAL
        walManager.close();
        System.out.println("存储引擎已关闭。");
    }

}
