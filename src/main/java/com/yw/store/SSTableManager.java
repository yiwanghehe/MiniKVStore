package com.yw.store;

import com.yw.skipList.SkipList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * 管理所有SSTable的生命周期、元数据和查询。
 */
public class SSTableManager {
    private final String sstDir;
    // 使用ConcurrentSkipListMap来存储每层的SSTable元数据,按文件名（时间戳）排序
    private final ConcurrentNavigableMap<Integer, ConcurrentSkipListMap<String, SSTableReader>> levels;
    private final AtomicLong nextSSTableId = new AtomicLong(0);

    public SSTableManager(String dataDir) {
        this.sstDir = dataDir + "/sst";
        this.levels = new ConcurrentSkipListMap<>();
    }

    public void loadSSTables() throws IOException {
        Files.createDirectories(Paths.get(sstDir));
        try (Stream<Path> stream = Files.list(Paths.get(sstDir))) {
            stream.filter(p -> p.toString().endsWith(".sst"))
                    .forEach(path -> {
                        try {
                            // 从文件名解析层级和ID
                            String fileName = path.getFileName().toString();
                            String[] parts = fileName.replace(".sst", "").split("-");
                            int level = Integer.parseInt(parts[0]);
                            long id = Long.parseLong(parts[1]);

                            nextSSTableId.set(Math.max(nextSSTableId.get(), id + 1));

                            SSTableReader reader = new SSTableReader(path.toString());
                            levels.computeIfAbsent(level, k -> new ConcurrentSkipListMap<>())
                                    .put(fileName, reader);
                        } catch (Exception e) {
                            System.out.println("加载SSTable失败: " + path + ", 原因: " + e.getMessage());
                        }
                    });
        }
        System.out.println("加载了 " + levels.values().stream().mapToLong(Map::size).sum() + " 个SSTable。");
    }

    public String get(String key) throws IOException {
        // 从L0层开始查找
        if (levels.containsKey(0)) {
            // L0的文件可能有重叠, 需要从新到旧查找
            // descendingMap返回降序视图（文件名数值大的在前）这样就能从最新文件开始查找
            for (SSTableReader reader : levels.get(0).descendingMap().values()) {
                String value = reader.get(key);
                if (value != null) return value;
            }
        }

        // 从L1层及以上查找
        for (int level = 1; levels.containsKey(level); level++) {
            ConcurrentSkipListMap<String, SSTableReader> levelFiles = levels.get(level);
            // L1+层的文件保证key不重叠，可以并行查找或利用元数据快速定位
            for (SSTableReader reader : levelFiles.values()) {
                if (key.compareTo(reader.getFirstKey()) >= 0 && key.compareTo(reader.getLastKey()) <= 0) {
                    String value = reader.get(key);
                    if (value != null) return value;
                }
            }
        }
        return null;
    }

    /**
     * 将一个MemTable的内容写入一个新的SSTable文件。
     *
     * @param memTable 要刷盘的MemTable
     * @throws IOException 写入失败
     */
    public void flushMemTableToSSTable(SkipList<String, String> memTable) throws IOException {
        if(memTable.getNodeCount() == 0) return;

        Long id = nextSSTableId.getAndIncrement();
        String fileName = String.format("0-%d.sst", id); // 新刷盘的文件总是在L0
        Path path = Paths.get(sstDir, fileName);

        SSTableWriter.write(memTable, path.toString());

        SSTableReader reader = new SSTableReader(path.toString());
        levels.computeIfAbsent(0, k -> new ConcurrentSkipListMap<>())
                .put(fileName, reader);

        System.out.println("刷盘完成: " + fileName);
    }

    public Map<Integer, ConcurrentSkipListMap<String, SSTableReader>> getLevels() {
        return levels;
    }

    public String getSstDir() {
        return sstDir;
    }

    public long getNextSSTableId() {
        return nextSSTableId.getAndIncrement();
    }
}
