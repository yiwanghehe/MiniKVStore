package com.yw.test;

import com.google.common.cache.CacheBuilder;
import com.yw.bloomfilter.MyBloomFilter;
import com.yw.store.LSMStore;
import com.yw.store.SSTableReader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * 一个在实际KV存储环境中验证布隆过滤器误判率的集成测试。
 */
public class BloomFilterFppTest {

    private static final String DATA_DIR = "./fpp_test_data";
    private static final long NUM_INSERTIONS = 1_000_000; // 插入100万条数据以生成SSTables
    private static final long TEST_SET_SIZE = 1_000_000; // 使用100万条不存在的数据进行测试

    public static void main(String[] args) throws Exception {
        System.out.println("--- SSTable 布隆过滤器集成测试 ---");

        // 步骤1: 搭建测试环境，写入数据以生成SSTable文件
        try {
            setupTestData();
        } catch (Exception e) {
            System.err.println("错误：测试数据准备阶段失败！");
            e.printStackTrace();
            return;
        }

        // 步骤2: 运行正确性测试 (检查假阴性)
        runCorrectnessTest();

        // 步骤3: 运行误判率测试 (检查假阳性)
        runFppTest();

        // 步骤4: 清理测试数据
        cleanup();

        System.out.println("\n--- 测试完成 ---");
    }

    /**
     * 启动一个LSMStore实例，写入大量数据，并关闭它以确保所有数据都被刷到SSTable中。
     */
    private static void setupTestData() throws IOException, InterruptedException {
        System.out.printf("\n[1/4] 正在准备测试数据，写入 %,d 条记录...\n", NUM_INSERTIONS);
        cleanup(); // 确保环境干净

        // 使用 try-with-resources 确保 store 被正确关闭
        try (LSMStore store = new LSMStore(DATA_DIR)) {
            for (long i = 0; i < NUM_INSERTIONS; i++) {
                store.put("inserted_key_" + i, "value_" + i);
            }
        } // store.close() 会在这里被自动调用，完成所有刷盘操作

        System.out.println("测试数据已成功写入 SSTable 文件。");
    }

    /**
     * 重新打开存储引擎，并GET所有已插入的键，以验证布隆过滤器没有产生“假阴性”（False Negative）。
     * 一个正确的布隆过滤器，其假阴性率必须为0。
     */
    private static void runCorrectnessTest() throws IOException, InterruptedException {
        System.out.printf("\n[2/4] 正在运行正确性测试 (检查假阴性)，查询 %,d 个已存在的键...\n", NUM_INSERTIONS);
        long falseNegatives = 0;

        try (LSMStore store = new LSMStore(DATA_DIR)) {
            for (long i = 0; i < NUM_INSERTIONS; i++) {
                if (i > 0 && i % 100_000 == 0) {
                    System.out.printf("    ...已检查 %,d 个键\n", i);
                }
                String key = "inserted_key_" + i;
                if (store.get(key) == null) {
                    falseNegatives++;
                    System.err.printf("!!! 严重错误 (假阴性): 插入的键 '%s' 未被找到！\n", key);
                }
            }
        }

        System.out.println("\n--- 正确性测试结果 ---");
        System.out.printf("检查总数: %,d\n", NUM_INSERTIONS);
        System.out.printf("未找到的键 (False Negatives): %,d\n", falseNegatives);
        System.out.println("------------------------------------");

        if (falseNegatives == 0) {
            System.out.println("测试通过：所有已插入的键都能被正确找到，没有假阴性问题。");
        } else {
            System.out.println("测试失败：存在假阴性，布隆过滤器或存储逻辑有严重BUG！");
        }
    }


    /**
     * 加载所有生成的SSTable文件，并用一系列确定不存在的键来测试它们的布隆过滤器，以计算“假阳性”概率（FPP）。
     */
    private static void runFppTest() throws IOException {
        System.out.printf("\n[3/4] 正在运行误判率测试 (检查假阳性)，使用 %,d 个不存在的键...\n", TEST_SET_SIZE);
        Path sstPath = Paths.get(DATA_DIR, "sst");
        if (!Files.exists(sstPath)) {
            System.err.println("错误：未找到 SSTable 目录，测试中止。");
            return;
        }

        // 加载所有SSTable的Reader
        List<SSTableReader> readers = new ArrayList<>();
        for (File sstFile : Objects.requireNonNull(sstPath.toFile().listFiles((dir, name) -> name.endsWith(".sst")))) {
            // 注意：这里我们使用一个临时的空缓存，因为我们只关心布隆过滤器，不关心数据块的读取性能。
            readers.add(new SSTableReader(sstFile.getAbsolutePath(), CacheBuilder.newBuilder().build()));
        }

        if (readers.isEmpty()) {
            System.err.println("错误：未找到任何 SSTable 文件，测试中止。");
            return;
        }
        System.out.printf("已成功加载 %,d 个 SSTable 文件进行测试。\n", readers.size());

        int falsePositives = 0;
        Field bloomFilterField;
        try {
            // 通过反射获取SSTableReader类中的私有bloomFilter字段
            bloomFilterField = SSTableReader.class.getDeclaredField("bloomFilter");
            // 允许我们访问私有字段
            bloomFilterField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("测试失败: 无法在 SSTableReader 中找到 'bloomFilter' 字段。", e);
        }

        // 遍历所有确定不存在的测试键
        for (long i = 0; i < TEST_SET_SIZE; i++) {
            String testKey = "non_existent_key_" + i;
            // 检查这个键是否会在任何一个SSTable的布隆过滤器中触发误判
            for (SSTableReader reader : readers) {
                try {
                    // 从reader实例中获取私有的bloomFilter对象
                    MyBloomFilter filter = (MyBloomFilter) bloomFilterField.get(reader);
                    if (filter != null && filter.mightContain(testKey)) {
                        // 只要有一个过滤器认为“可能存在”，就算一次误判
                        falsePositives++;
                        break; // 无需再检查其他文件，直接测试下一个key
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("测试失败: 无法通过反射访问 'bloomFilter' 字段。", e);
                }
            }
        }

        double actualFpp = (double) falsePositives / TEST_SET_SIZE;
        double targetFpp = 0.01; // 我们在代码中硬编码的目标误判率

        System.out.println("\n--- 误判率测试结果 ---");
        System.out.printf("测试查询总数: %,d\n", TEST_SET_SIZE);
        System.out.printf("错误判断数 (False Positives): %,d\n", falsePositives);
        System.out.printf("实际计算出的误判率 (Actual FPP): %.6f (%.4f%%)\n", actualFpp, actualFpp * 100);
        System.out.println("------------------------------------");
    }

    /**
     * 清理测试目录。
     */
    private static void cleanup() {
        Path dataPath = Paths.get(DATA_DIR);
        if (Files.exists(dataPath)) {
            try {
                Files.walk(dataPath)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                System.err.println("清理环境失败: " + e.getMessage());
            }
        }
    }
}

