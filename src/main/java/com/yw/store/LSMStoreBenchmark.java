package com.yw.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A benchmark for testing the performance of LSMStore.
 * Measures Insertions Per Second (IPS) and Queries Per Second (QPS).
 */
public class LSMStoreBenchmark {

    // --- 配置 ---
    private static final String DATA_DIR = "./benchmark_data";
    private static final int NUM_THREADS = 10; // 并发线程数
    private static final int OPERATIONS_PER_THREAD = 50000; // 每个线程执行的操作数
    private static final int TOTAL_OPERATIONS = NUM_THREADS * OPERATIONS_PER_THREAD;

    public static void main(String[] args) throws Exception {
        System.out.println("--- MiniKVStore 性能基准测试 ---");
        System.out.printf("配置: %d 个线程, 每个线程执行 %,d 次操作 (共 %,d 次)\n",
                NUM_THREADS, OPERATIONS_PER_THREAD, TOTAL_OPERATIONS);
        System.out.println("---------------------------------------------------");

        // 运行写性能测试
        runWriteBenchmark();

        // 运行读性能测试
        runReadBenchmark();

        System.out.println("\n--- 基准测试完成 ---");
    }

    /**
     * 运行写性能基准测试 (IPS).
     */
    private static void runWriteBenchmark() throws Exception {
        System.out.println(">>> [1/2] 开始写入性能测试 (IPS)...");
        cleanup(); // 确保环境干净

        LSMStore store = new LSMStore(DATA_DIR);
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        String key = "bench_key_" + threadId + "_" + j;
                        String value = generateRandomString(128); // 写入128字节的随机值
                        store.put(key, value);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        double ips = (duration > 0) ? (double) TOTAL_OPERATIONS * 1000 / duration : 0;

        System.out.printf("写入测试完成。\n");
        System.out.printf("    总耗时: %,d ms\n", duration);
        System.out.printf("    每秒插入操作数 (IPS): %,.2f\n", ips);

        executor.shutdown();
        store.close();
    }

    /**
     * 运行读性能基准测试 (QPS).
     */
    private static void runReadBenchmark() throws Exception {
        System.out.println("\n>>> [2/2] 开始读取性能测试 (QPS)...");
        // 注意：读测试依赖于写测试产生的数据，因此它应该在写测试之后运行
        // 如果要独立运行，需要先手动准备数据

        LSMStore store = new LSMStore(DATA_DIR);
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(NUM_THREADS);

        System.out.println("数据预热和加载已完成，开始读取测试...");

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < OPERATIONS_PER_THREAD; j++) {
                        // 随机读取一个已存在的key
                        int randomThreadId = ThreadLocalRandom.current().nextInt(NUM_THREADS);
                        int randomOpId = ThreadLocalRandom.current().nextInt(OPERATIONS_PER_THREAD);
                        String key = "bench_key_" + randomThreadId + "_" + randomOpId;
//                        System.out.println(store.get(key));
                        store.get(key);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        double qps = (duration > 0) ? (double) TOTAL_OPERATIONS * 1000 / duration : 0;

        System.out.printf("读取测试完成。\n");
        System.out.printf("    总耗时: %,d ms\n", duration);
        System.out.printf("    每秒查询操作数 (QPS): %,.2f\n", qps);

        executor.shutdown();
        store.close();
        cleanup(); // 测试完成后清理
    }


    /**
     * 生成指定长度的随机字符串.
     * @param length 字符串长度
     * @return 随机字符串
     */
    private static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = ThreadLocalRandom.current().nextInt(characters.length());
            result.append(characters.charAt(index));
        }
        return result.toString();
    }

    /**
     * 清理测试目录
     */
    private static void cleanup() {
        Path dataPath = Paths.get(DATA_DIR);
        if (Files.exists(dataPath)) {
            try {
                Files.walk(dataPath)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(file -> {
                            if (!file.delete()) {
                                System.err.println("无法删除文件: " + file.getAbsolutePath());
                            }
                        });
                System.out.println("清理环境: 删除了 '" + DATA_DIR + "' 目录。");
            } catch (IOException e) {
                System.err.println("清理环境失败: " + e.getMessage());
            }
        }
    }
}
