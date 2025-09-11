package com.yw;

import com.yw.ConcurrentSkipListMap.ConcurrentSkipListMapKVStore;
import com.yw.skipList.SkipList;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 一个用于比较自定义SkipList实现与Java内置ConcurrentSkipListMap性能的基准测试类。
 */
public class Benchmark {
    // --- 配置 ---
    public static final int INSERT_TIMES_PER_THREAD = 100000; // 每个线程插入一百万条记录
    public static final int SEARCH_TIMES_PER_THREAD = 100000; // 每个线程搜索一百万条记录
    public static final int NUM_THREADS = 10;
    public static final int TOTAL_OPERATIONS = NUM_THREADS * INSERT_TIMES_PER_THREAD;

    /**
     * 生成一个固定长度的随机字母数字字符串。
     */
    public static String generateRandomString() {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        int length = 10;
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = ThreadLocalRandom.current().nextInt(characters.length());
            result.append(characters.charAt(index));
        }
        return result.toString();
    }

    /**
     * 一个在给定键值存储上执行插入操作的通用任务。
     */
    private static class InsertTask implements Runnable {
        private final Object store;

        InsertTask(Object store) {
            this.store = store;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            if (store instanceof SkipList) {
                SkipList<String, String> skipList = (SkipList<String, String>) store;
                for (int i = 0; i < INSERT_TIMES_PER_THREAD; i++) {
                    skipList.insert(generateRandomString(), generateRandomString());
                }
            } else if (store instanceof ConcurrentSkipListMapKVStore) {
                ConcurrentSkipListMapKVStore<String, String> mapStore = (ConcurrentSkipListMapKVStore<String, String>) store;
                for (int i = 0; i < INSERT_TIMES_PER_THREAD; i++) {
                    mapStore.insert(generateRandomString(), generateRandomString());
                }
            }
        }
    }

    /**
     * 一个在给定键值存储上执行搜索操作的通用任务。
     */
    private static class SearchTask implements Runnable {
        private final Object store;

        SearchTask(Object store) {
            this.store = store;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            if (store instanceof SkipList) {
                SkipList<String, String> skipList = (SkipList<String, String>) store;
                for (int i = 0; i < SEARCH_TIMES_PER_THREAD; i++) {
                    skipList.search(generateRandomString());
                }
            } else if (store instanceof ConcurrentSkipListMapKVStore) {
                ConcurrentSkipListMapKVStore<String, String> mapStore = (ConcurrentSkipListMapKVStore<String, String>) store;
                for (int i = 0; i < SEARCH_TIMES_PER_THREAD; i++) {
                    mapStore.search(generateRandomString());
                }
            }
        }
    }

    /**
     * 基准测试的主入口点。
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println("基准测试开始，使用 " + NUM_THREADS + " 个线程。");
        System.out.println("每次测试的总操作数: " + String.format("%,d", TOTAL_OPERATIONS));
        System.out.println("---------------------------------------------------");

        // --- 对自定义 SkipList 进行基准测试 ---
        System.out.println("正在测试自定义 SkipList (使用读写锁)...");
        SkipList<String, String> customSkipList = new SkipList<>();
        runBenchmark(customSkipList, "自定义 SkipList");
        System.out.println("---------------------------------------------------");

        // --- 对 ConcurrentSkipListMap 进行基准测试 ---
        System.out.println("正在测试 ConcurrentSkipListMap (无锁)...");
        ConcurrentSkipListMapKVStore<String, String> cslmStore = new ConcurrentSkipListMapKVStore<>();
        runBenchmark(cslmStore, "ConcurrentSkipListMap");
        System.out.println("---------------------------------------------------");
    }

    /**
     * 为给定的存储运行插入和搜索基准测试并计时。
     */
    private static void runBenchmark(Object store, String storeName) throws InterruptedException {
        // --- 插入测试 ---
        long startTime = System.currentTimeMillis();
        Thread[] insertThreads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            insertThreads[i] = new Thread(new InsertTask(store));
            insertThreads[i].start();
        }
        for (Thread t : insertThreads) {
            t.join();
        }
        long endTime = System.currentTimeMillis();
        long insertDuration = endTime - startTime;
        long insertOpsPerSecond = (insertDuration > 0) ? (TOTAL_OPERATIONS * 1000 / insertDuration) : 0;
        System.out.printf("[%s] 插入测试完成。\n", storeName);
        System.out.printf("    总耗时: %d ms\n", insertDuration);
        System.out.printf("    每秒操作数 (IPS): %,d\n", insertOpsPerSecond);

        // --- 搜索测试 ---
        startTime = System.currentTimeMillis();
        Thread[] searchThreads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            searchThreads[i] = new Thread(new SearchTask(store));
            searchThreads[i].start();
        }
        for (Thread t : searchThreads) {
            t.join();
        }
        endTime = System.currentTimeMillis();
        long searchDuration = endTime - startTime;
        long searchOpsPerSecond = (searchDuration > 0) ? (TOTAL_OPERATIONS * 1000 / searchDuration) : 0;
        System.out.printf("[%s] 搜索测试完成。\n", storeName);
        System.out.printf("    总耗时: %d ms\n", searchDuration);
        System.out.printf("    每秒操作数 (QPS): %,d\n", searchOpsPerSecond);
    }
}

