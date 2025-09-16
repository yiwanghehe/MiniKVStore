package com.yw.test;

import com.yw.store.LSMStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个全面的测试套件，用于验证LSMStore的功能、持久性和并发正确性。
 */
public class ComprehensiveTest {

    private static final String DATA_DIR = "./data";

    public static void main(String[] args) throws Exception {
        System.out.println("--- 开始 MiniKVStore 综合测试 ---");

        // 每次测试前清理环境
        cleanup();

        // 运行测试场景
        testBasicPutGetDelete();
        testPersistence();
        testMemTableFlush();
        testCompaction();
        testConcurrency();

        // 清理测试产生的数据
        cleanup();

        System.out.println("\n--- 所有测试成功通过！ ---");
    }

    /**
     * 测试1：基本 Put, Get, Delete 功能
     */
    private static void testBasicPutGetDelete() throws Exception {
        System.out.println("\n--- [测试 1/5] 基本 Put/Get/Delete 功能 ---");
        try (LSMStore store = new LSMStore()) {
            System.out.println("写入 key1 = value1");
            store.put("key1", "value1");
            assert "value1".equals(store.get("key1")) : "读取 key1 失败";
            System.out.println("读取 key1 -> " + store.get("key1") + " (正确)");

            System.out.println("更新 key1 = value1_updated");
            store.put("key1", "value1_updated");
            assert "value1_updated".equals(store.get("key1")) : "更新 key1 失败";
            System.out.println("读取 key1 -> " + store.get("key1") + " (正确)");

            System.out.println("删除 key1");
            store.delete("key1");
            assert store.get("key1") == null : "删除 key1 失败";
            System.out.println("读取 key1 -> null (正确)");

            System.out.println("读取不存在的 key_non_exist -> null (正确)");
            assert store.get("key_non_exist") == null;
        }
        System.out.println("[通过] 基本功能测试完成。");
    }

    /**
     * 测试2：持久化和恢复功能
     */
    private static void testPersistence() throws Exception {
        System.out.println("\n--- [测试 2/5] 持久化与恢复功能 ---");
        try (LSMStore store = new LSMStore()) {
            store.put("persist_key", "persist_value");
            store.put("persist_key_2", "persist_value_2");
            System.out.println("写入两条持久化数据并关闭引擎...");
        } // 'try-with-resources' 会自动调用 store.close()

        System.out.println("重新启动引擎...");
        try (LSMStore store = new LSMStore()) {
            System.out.println("读取 persist_key -> " + store.get("persist_key"));
            assert "persist_value".equals(store.get("persist_key")) : "恢复 persist_key 失败";

            System.out.println("读取 persist_key_2 -> " + store.get("persist_key_2"));
            assert "persist_value_2".equals(store.get("persist_key_2")) : "恢复 persist_key_2 失败";
        }
        System.out.println("[通过] 持久化与恢复测试完成。");
    }

    /**
     * 测试3：MemTable 自动刷盘
     */
    private static void testMemTableFlush() throws Exception {
        System.out.println("\n--- [测试 3/5] MemTable 自动刷盘 ---");
        // 注意：这个测试依赖于 LSMStore 中的 MEMTABLE_THRESHOLD 配置
        // 为了方便测试，可以临时将其改小，例如 1024 (1KB)
        try (LSMStore store = new LSMStore()) {
            // 生成一个大约 5KB 的字符串
            String largeValue = "v".repeat(5 * 1024);
            // 写入大量数据以触发刷盘
            for (int i = 0; i < 1000; i++) {
                store.put("flush_key_" + i, "value_" + i);
            }
            System.out.println("写入大量数据触发MemTable刷盘...");
            Thread.sleep(1000); // 等待后台刷盘线程执行

            Path sstDir = Paths.get(DATA_DIR, "sst");
            assert Files.list(sstDir).count() > 0 : "没有SSTable文件生成，刷盘失败";
            System.out.println("检查到SSTable文件已生成。");

            // 验证数据是否仍然可读
            assert "value_500".equals(store.get("flush_key_500")) : "从SSTable读取数据失败";
            System.out.println("读取 flush_key_500 -> " + store.get("flush_key_500") + " (正确)");
        }
        System.out.println("[通过] MemTable 刷盘测试完成。");
    }

    /**
     * 测试4：Compaction 合并
     */
    private static void testCompaction() throws Exception {
        System.out.println("\n--- [测试 4/5] Compaction 合并 ---");
        // 需要 L0_COMPACTION_THRESHOLD = 4
        try (LSMStore store = new LSMStore()) {
            // 制造多个L0 SSTable文件
            for (int i = 0; i < 5; i++) {
                // 每次写入足够的数据以填满一个MemTable并刷盘
                for (int j = 0; j < 1000; j++) {
                    store.put("compact_key_" + i + "_" + j, "value");
                }
                // 手动切换并等待刷盘
                // 这是一个简化，实际应依赖后台线程，这里为了测试确定性
                store.put("force_switch_" + i, "v");
                Thread.sleep(500); // 确保刷盘线程有机会运行
            }

            System.out.println("已生成多个L0 SSTable，等待Compaction线程执行...");
            Thread.sleep(6000); // CompactionManager 每5秒检查一次，所以等待6秒

            Path sstDir = Paths.get(DATA_DIR, "sst");
            long l1Files = Files.list(sstDir).filter(p -> p.getFileName().toString().startsWith("1-")).count();
            assert l1Files > 0 : "Compaction后没有生成L1文件";
            System.out.println("检查到L1层文件已生成。");

            // 验证数据
            assert "value".equals(store.get("compact_key_1_1")) : "Compaction后数据读取失败";
            System.out.println("读取 compact_key_1_1 -> " + store.get("compact_key_1_1") + " (正确)");

            // 写入一个将被删除的数据
            store.put("key_to_be_compacted_away", "old_value");
            store.delete("key_to_be_compacted_away");

            // 再次触发合并，确保删除生效
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 1000; j++) {
                    store.put("compact_key_again_" + i + "_" + j, "value");
                }
                store.put("force_switch_again" + i, "v");
                Thread.sleep(500);
            }
            Thread.sleep(6000);
            assert store.get("key_to_be_compacted_away") == null : "删除的数据在Compaction后依然存在";
            System.out.println("验证被删除的数据在Compaction后已清理 (正确)");
        }
        System.out.println("[通过] Compaction 合并测试完成。");
    }

    /**
     * 测试5：并发读写
     */
    private static void testConcurrency() throws Exception {
        System.out.println("\n--- [测试 5/5] 并发读写 ---");
        final int numThreads = 10;
        final int opsPerThread = 50000;
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final AtomicInteger errors = new AtomicInteger(0);
        final LSMStore store = new LSMStore();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        System.out.printf("启动 %d 个线程，每个线程执行 %d 次随机读写操作...\n", numThreads, opsPerThread);

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < opsPerThread; j++) {
                        String key = "concurrent_key_" + threadId + "_" + j;
                        String value = UUID.randomUUID().toString();

                        // 随机进行读或写
                        if (Math.random() > 0.5) {
                            store.put(key, value);
                            if (!value.equals(store.get(key))) {
                                System.err.println("并发错误: 写入后读取的值不匹配 for key " + key);
                                errors.incrementAndGet();
                            }
                        } else {
                            // 随机读一个可能存在的key
                            int randomThread = (int)(Math.random() * numThreads);
                            int randomOp = (int)(Math.random() * opsPerThread);
                            store.get("concurrent_key_" + randomThread + "_" + randomOp);
                        }
                    }
                } catch (IOException e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        store.close();

        assert errors.get() == 0 : "并发测试中出现 " + errors.get() + " 个错误";
        System.out.println("[通过] 并发读写测试完成，没有错误。");
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
                            boolean deleted = file.delete();
                            if (!deleted) {
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
