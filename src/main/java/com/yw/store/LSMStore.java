package com.yw.store;

import com.yw.skipList.SkipList;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LSM-Tree 架构的核心存储引擎 (V2)。
 * 协调 WAL, MemTable, SSTableManager, 和 CompactionManager。
 */
public class LSMStore implements AutoCloseable {
    // --- 核心组件 ---
    private volatile SkipList<String, String> activeMemTable; // 当前活跃的可写内存表
    private final ConcurrentLinkedQueue<SkipList<String, String>> immutableMemTables; // 只读的、等待刷盘的内存表队列
    private final WALManager<String, String> walManager; // 预写日志管理器
    private final SSTableManager sstManager; // 管理所有SSTable的生命周期、元数据和查询。
    private final CompactionManager compactionManager; // 后台线程，负责执行SSTable的合并操作。

    // --- 配置与状态 ---
    public static final String TOMBSTONE = "!!__TOMBSTONE__!!"; // 删除标记(墓碑)
    private static final String DATA_DIR = "./data"; // 数据存储目录
    private static final long MEMTABLE_THRESHOLD = 4 * 1024 * 1024; // MemTable切换阈值 (4MB)
    private final ReadWriteLock memtableSwitchLock = new ReentrantReadWriteLock(); // MemTable切换锁
    private volatile boolean shuttingDown = false; // 关闭的状态标志

    // --- 后台任务 ---
    private final ExecutorService flushExecutor; // 用于执行刷盘任务的单线程执行器

    /**
     * 构造函数，初始化LSM存储引擎。
     *
     * @throws IOException 初始化过程中发生IO错误
     */
    public LSMStore() throws IOException {
        this.activeMemTable = new SkipList<>();
        this.immutableMemTables = new ConcurrentLinkedQueue<>();
        this.walManager = new WALManager<>(DATA_DIR);
        this.sstManager = new SSTableManager(DATA_DIR);

        // 恢复未完成刷盘的MemTable
        recoverFromWAL();
        sstManager.loadSSTables(); // 加载现有SSTable的元数据

        // 启动后台刷盘线程
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lsm-flush-thread");
            t.setDaemon(true); // 设置为守护线程,不会阻止JVM的退出
            return t;
        });
        this.flushExecutor.submit(this::flushTask);

        // 后台合并线程
        this.compactionManager = new CompactionManager(sstManager);
        this.compactionManager.start();
    }

    /**
     * 向存储引擎中插入或更新一个键值对。
     *
     * @param key   键
     * @param value 值
     * @throws IOException WAL写入失败
     */
    public void put(String key, String value) throws IOException {
        if (shuttingDown) throw new IllegalStateException("存储引擎正在关闭，无法接受新的写入。");
        if (key == null || value == null) throw new IllegalArgumentException("Key和Value不能为空");

        memtableSwitchLock.readLock().lock();
        try {
            // 先写入WAL日志，保证数据不丢失
            walManager.logPut(key, value);
            // 写入MemTable
            activeMemTable.insert(key, value);
            if (activeMemTable.getApproximateSize() >= MEMTABLE_THRESHOLD) {
                // 升级读锁为写锁前，必须先释放读锁
                memtableSwitchLock.readLock().unlock();
                memtableSwitchLock.writeLock().lock();
                try {
                    // 双重检查，防止在等待写锁时其他线程已经完成了切换
                    // 检查是否切换MemTable
                    if (activeMemTable.getApproximateSize() >= MEMTABLE_THRESHOLD) {
                        switchMemTable();
                    }
                } finally {
                    // 降级写锁为读锁
                    memtableSwitchLock.readLock().lock();
                    memtableSwitchLock.writeLock().unlock();
                }
            }
        } finally {
            memtableSwitchLock.readLock().unlock();
        }
    }

    public void delete(String key) throws IOException {
        if (shuttingDown) throw new IllegalStateException("引擎正在关闭");
        if (key == null) throw new IllegalArgumentException("Key不能为空");

        put(key, TOMBSTONE);
    }

    /**
     * 切换MemTable，将当前活跃的MemTable变为不可变，并创建一个新的空MemTable。
     */
    private void switchMemTable() throws IOException {
        // 这个方法必须在持有写锁的情况下被调用

        // 防止并发切换
        if (activeMemTable.getNodeCount() == 0) return;
        // 将当前的MemTable放入不可变队列，等待刷盘
        immutableMemTables.add(activeMemTable);
        // 新初始化一个MemTable
        activeMemTable = new SkipList<>();
        // 轮转日志
        walManager.rotateLog();
    }

    /**
     * 从存储引擎中获取一个键对应的值。
     *
     * @param key 键
     * @return 值，如果不存在则返回null
     * @throws IOException 读取SSTable失败
     */
    public String get(String key) throws IOException {
        if (key == null) return null;

        String value;

        // 先从活跃的MemTable中查找
        memtableSwitchLock.readLock().lock();
        try {
            value = activeMemTable.get(key);
        } finally {
            memtableSwitchLock.readLock().unlock();
        }
        if (value != null) return value.equals(TOMBSTONE) ? null : value;

        // 再从不可变的immutableMemTableQueue中查找(从新到旧)
        for (SkipList<String, String> memTable : immutableMemTables) {
            value = memTable.get(key);
            if (value != null) return value.equals(TOMBSTONE) ? null : value;
        }

        // 从SSTable中查找
        value = sstManager.get(key);
        return (value != null && value.equals(TOMBSTONE)) ? null : value;
    }

    /**
     * 后台刷盘任务，循环执行。
     */
    private void flushTask() {
        while (!shuttingDown || !immutableMemTables.isEmpty()) {
            SkipList<String, String> memTableToFlush = immutableMemTables.poll();
            if (memTableToFlush != null) {
                try {
                    sstManager.flushMemTableToSSTable(memTableToFlush);
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
     * 从WAL日志中恢复数据。在引擎启动时调用。
     */
    private void recoverFromWAL() throws IOException {
        walManager.recover(activeMemTable);
    }

    /**
     * 关闭存储引擎，确保所有数据都已持久化。
     */
    public void close() throws InterruptedException, IOException {
        System.out.println("正在关闭存储引擎...");

        // 设置关闭标志，并拒绝新的写入
        shuttingDown = true;

        compactionManager.shutdown();

        // 切换最后的activeMemTable
        memtableSwitchLock.writeLock().lock();
        try {
            // 将最后的 activeMemTable(如果非空)也加入待刷盘队列
            if (activeMemTable.getNodeCount() > 0) {
                immutableMemTables.add(activeMemTable);
                activeMemTable = new SkipList<>();
            }
        } finally {
            memtableSwitchLock.writeLock().unlock();
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
