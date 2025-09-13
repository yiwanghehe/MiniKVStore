package com.yw.store;

import com.yw.node.Node;
import com.yw.skipList.SkipList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * 管理所有SSTable的生命周期、元数据和查询。
 */
public class SSTableManager {
    private final String sstDir;
    // 使用ConcurrentSkipListMap来存储每层的SSTable元数据,按文件名（时间戳）排序
    private final ConcurrentNavigableMap<Integer, ConcurrentSkipListMap<String, SSTableReader>> levels;
    private final ReadWriteLock metadataLock = new ReentrantReadWriteLock(); // 用于保护levels元数据和物理文件在合并期间不被并发访问的读写锁
    private final AtomicLong nextSSTableId = new AtomicLong(0); // 下一个SSTable文件的id
    private static final int L0_COMPACTION_THRESHOLD = 4; // L0文件数量达到4个时触发合并

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
        metadataLock.readLock().lock();
        try {
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
                    // 确保firstKey和lastKey不为null
                    if (reader.getFirstKey() != null && reader.getLastKey() != null &&
                            key.compareTo(reader.getFirstKey()) >= 0 && key.compareTo(reader.getLastKey()) <= 0) {
                        String value = reader.get(key);
                        if (value != null) return value;
                    }
                }
            }
            return null;
        } finally {
            metadataLock.readLock().unlock();
        }

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
        // 写锁
        metadataLock.writeLock().lock();
        try {
            levels.computeIfAbsent(0, k -> new ConcurrentSkipListMap<>())
                    .put(fileName, reader);
        } finally {
            metadataLock.writeLock().unlock();
        }
        System.out.println("刷盘完成: " + fileName);
    }

    /**
     * 执行Compaction的核心逻辑，现在是SSTableManager的线程安全方法。
     */
    public void compact() throws IOException {
        metadataLock.writeLock().lock(); // 获取写锁，阻塞所有读写操作
        try {
            ConcurrentSkipListMap<String, SSTableReader> l0Files = levels.get(0);
            if (l0Files == null || l0Files.size() < L0_COMPACTION_THRESHOLD) {
                return; // 不需要合并
            }
            System.out.println("触发L0 Compaction，文件数: " + l0Files.size());

            // --- 1. 选择要合并的文件 ---
            List<SSTableReader> filesToCompact = new ArrayList<>(l0Files.values());
            String minKey = filesToCompact.stream().map(SSTableReader::getFirstKey).filter(Objects::nonNull).min(Comparator.naturalOrder()).orElse(null);
            String maxKey = filesToCompact.stream().map(SSTableReader::getLastKey).filter(Objects::nonNull).max(Comparator.naturalOrder()).orElse(null);
            if (minKey == null) return;

            // --- 2. 选择L1中键范围重叠的文件 ---
            if (levels.containsKey(1)) {
                for (SSTableReader readerL1 : levels.get(1).values()) {
                    if (!(readerL1.getLastKey().compareTo(minKey) < 0 || readerL1.getFirstKey().compareTo(maxKey) > 0)) {
                        filesToCompact.add(readerL1);
                    }
                }
            }

            // --- 3 & 4. 合并并写入新SSTable ---
            String newSstFileName = String.format("1-%d.sst", getNextSSTableId());
            Path newSstPath = Paths.get(sstDir, newSstFileName);
            mergeAndWrite(filesToCompact, newSstPath);

            // --- 5. 原子地更新元数据 ---
            SSTableReader newReader = new SSTableReader(newSstPath.toString());
            levels.computeIfAbsent(1, k -> new ConcurrentSkipListMap<>()).put(newSstFileName, newReader);

            for (SSTableReader reader : filesToCompact) {
                String fileName = Paths.get(reader.getFilePath()).getFileName().toString();
                int level = Integer.parseInt(fileName.split("-")[0]);
                if(levels.containsKey(level)) {
                    levels.get(level).remove(fileName);
                }

                // 在删除物理文件前，先关闭文件句柄
                reader.close();
                Files.delete(Paths.get(reader.getFilePath()));
            }

            System.out.println("Compaction 完成，生成新文件: " + newSstFileName);

        } finally {
            metadataLock.writeLock().unlock(); // 释放写锁
        }
    }

    private void mergeAndWrite(List<SSTableReader> readers, Path outputPath) throws IOException {
        // ... (此部分为私有辅助方法，在写锁保护下调用) ...
//        PriorityQueue<CompactionManager.IteratorEntry> pq = new PriorityQueue<>();
//        for (SSTableReader reader : readers) {
//            SSTableIterator it = reader.iterator();
//            if (it.hasNext()) {
//                pq.add(new CompactionManager.IteratorEntry(it.next(), it));
//            }
//        }
//
//        SkipList<String, String> mergedData = new SkipList<>();
//        String lastKey = null;
//
//        while(!pq.isEmpty()) {
//            CompactionManager.IteratorEntry entry = pq.poll();
//            Node<String, String> node = entry.node;
//
//            if (!node.getKey().equals(lastKey)) {
//                if(!node.getValue().equals(LSMStore.TOMBSTONE)) {
//                    mergedData.insert(node.getKey(), node.getValue());
//                }
//                lastKey = node.getKey();
//            }
//
//            if (entry.iterator.hasNext()) {
//                entry.node = entry.iterator.next();
//                pq.add(entry);
//            } else {
//                entry.iterator.close();
//            }
//        }
//
//        if (mergedData.getNodeCount() > 0) {
//            SSTableWriter.write(mergedData, outputPath.toString());
//        }
        List<SSTableIterator> iterators = new ArrayList<>();
        PriorityQueue<CompactionManager.IteratorEntry> pq = new PriorityQueue<>();
        try {
            for (SSTableReader reader : readers) {
                SSTableIterator it = reader.iterator();
                iterators.add(it); // 跟踪所有迭代器以便最后关闭
                if (it.hasNext()) {
                    pq.add(new CompactionManager.IteratorEntry(it.next(), it));
                }
            }

            SkipList<String, String> mergedData = new SkipList<>();
            String lastKey = null;

            while (!pq.isEmpty()) {
                CompactionManager.IteratorEntry entry = pq.poll();
                Node<String, String> node = entry.node;

                // 跳过重复的、旧版本的key
                if (lastKey == null || !node.getKey().equals(lastKey)) {
                    // 如果不是删除标记（墓碑），就加入结果集
                    if (!node.getValue().equals(LSMStore.TOMBSTONE)) {
                        mergedData.insert(node.getKey(), node.getValue());
                    }
                    lastKey = node.getKey();
                }

                if (entry.iterator.hasNext()) {
                    entry.node = entry.iterator.next();
                    pq.add(entry);
                }
            }

            if (mergedData.getNodeCount() > 0) {
                SSTableWriter.write(mergedData, outputPath.toString());
            }
        } finally {
            // 确保所有打开的迭代器都被关闭，防止文件句柄泄露
            for (SSTableIterator it : iterators) {
                try {
                    it.close();
                } catch (IOException e) {
                    System.err.println("Compaction: 关闭迭代器失败: " + e.getMessage());
                }
            }
        }
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

    /**
     * 关闭管理器，释放所有文件句柄。
     */
    public void close() throws IOException {
        metadataLock.writeLock().lock();
        try {
            for (Map<String, SSTableReader> level : levels.values()) {
                for (SSTableReader reader : level.values()) {
                    reader.close();
                }
            }
            levels.clear();
            System.out.println("所有SSTable文件句柄已关闭。");
        } finally {
            metadataLock.writeLock().unlock();
        }
    }
}
