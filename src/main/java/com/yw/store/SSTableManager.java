package com.yw.store;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.yw.bloomfilter.MyBloomFilter;
import com.yw.node.Node;
import com.yw.skipList.SkipList;
import com.yw.store.entry.IndexEntry;
import com.yw.store.entry.IteratorEntry;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    private final ReadWriteLock metadataLock = new ReentrantReadWriteLock(true); // 用于保护levels元数据和物理文件在合并期间不被并发访问的读写锁
    private final AtomicLong nextSSTableId = new AtomicLong(0); // 下一个SSTable文件的id
    private static final int L0_COMPACTION_THRESHOLD = 5; // L0文件数量触发合并时的阈值

    private final Cache<String, byte[]> blockCache; // 共享的数据块缓存

    public SSTableManager(String dataDir) {
        this.sstDir = dataDir + "/sst";
        this.levels = new ConcurrentSkipListMap<>();

        // 初始化LRU缓存
        this.blockCache = CacheBuilder.newBuilder()
                .maximumSize(1000000)
                .build();
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
                            SSTableReader reader = new SSTableReader(path.toString(), blockCache);
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
            // 往L1层及以上查找
            for (int level = 1; levels.containsKey(level); level++) {
                ConcurrentSkipListMap<String, SSTableReader> levelFiles = levels.get(level);
                // L1+层的文件保证key不重叠，可以并行查找或利用元数据快速定位
                for (SSTableReader reader : levelFiles.values()) {
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

    public void flushMemTableToSSTable(SkipList<String, String> memTable) throws IOException {
        if (memTable.getNodeCount() == 0) return;
        Long id = nextSSTableId.getAndIncrement();
        String fileName = String.format("0-%d.sst", id);
        Path path = Paths.get(sstDir, fileName);
        SSTableWriter.write(memTable, path.toString());
        SSTableReader reader = new SSTableReader(path.toString(), blockCache);
        metadataLock.writeLock().lock();
        try {
            levels.computeIfAbsent(0, k -> new ConcurrentSkipListMap<>())
                    .put(fileName, reader);
        } finally {
            metadataLock.writeLock().unlock();
        }
        System.out.println("刷盘完成: " + fileName);
    }

    public void compact() throws IOException {
        metadataLock.writeLock().lock();
        try {
            ConcurrentSkipListMap<String, SSTableReader> l0Files = levels.get(0);
            if (l0Files == null || l0Files.size() < L0_COMPACTION_THRESHOLD) {
                return;
            }
            System.out.println("触发L0 Compaction，文件数: " + l0Files.size());

            // 选择要合并的文件
            List<SSTableReader> filesToCompact = new ArrayList<>(l0Files.values());
            String minKey = filesToCompact.stream().map(SSTableReader::getFirstKey).filter(Objects::nonNull).min(Comparator.naturalOrder()).orElse(null);
            String maxKey = filesToCompact.stream().map(SSTableReader::getLastKey).filter(Objects::nonNull).max(Comparator.naturalOrder()).orElse(null);
            if (minKey == null) return;

            // 选择L1中键范围重叠的文件
            if (levels.containsKey(1)) {
                for (SSTableReader readerL1 : levels.get(1).values()) {
                    if (!(readerL1.getLastKey().compareTo(minKey) < 0 || readerL1.getFirstKey().compareTo(maxKey) > 0)) {
                        filesToCompact.add(readerL1);
                    }
                }
            }

            // 合并并写入新SSTable
            String newSstFileName = String.format("1-%d.sst", getNextSSTableId());
            Path newSstPath = Paths.get(sstDir, newSstFileName);
            boolean createdNewFile = mergeAndWriteStream(filesToCompact, newSstPath);

            if (createdNewFile) {
                SSTableReader newReader = new SSTableReader(newSstPath.toString(), blockCache);
                levels.computeIfAbsent(1, k -> new ConcurrentSkipListMap<>()).put(newSstFileName, newReader);
            }

            for (SSTableReader reader : filesToCompact) {
                String fileName = Paths.get(reader.getFilePath()).getFileName().toString();
                int level = Integer.parseInt(fileName.split("-")[0]);
                if (levels.containsKey(level)) {
                    levels.get(level).remove(fileName);
                }
                // 在删除物理文件前，先关闭文件句柄
                reader.close();
            }

            for (SSTableReader reader : filesToCompact) {
                // 从缓存中移除已失效SSTable的所有数据块
                reader.invalidateCache();
                Files.delete(Paths.get(reader.getFilePath()));
            }

            System.out.println("Compaction 完成，生成新文件: " + (createdNewFile ? newSstFileName : "(无，所有数据均被清理)"));
        } catch (Exception e) {
            System.err.println("Compaction 内部发生严重错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    private boolean mergeAndWriteStream(List<SSTableReader> readers, Path outputPath) throws IOException {
        List<SSTableIterator> iterators = new ArrayList<>();
        PriorityQueue<IteratorEntry> pq = new PriorityQueue<>();

        try (FileOutputStream fos = new FileOutputStream(outputPath.toFile());
             DataOutputStream dos = new DataOutputStream(fos)) {

            for (SSTableReader reader : readers) {
                SSTableIterator it = reader.iterator();
                iterators.add(it);
                if (it.hasNext()) {
                    String fileName = Paths.get(reader.getFilePath()).getFileName().toString();
                    long fileId = Long.parseLong(fileName.replace(".sst", "").split("-")[1]);
                    pq.add(new IteratorEntry(it.next(), it, fileId));
                }
            }

            List<IndexEntry> indexs = new ArrayList<>();
            MyBloomFilter bloomFilter = MyBloomFilter.create(1_000_000, 0.01);
            long entriesInSSTable = 0;
            String lastKey = null;

            while (!pq.isEmpty()) {
                long blockStartOffset = dos.size();
                String lastKeyInBlock = null;
                int currentBlockSize = 0;

                while (!pq.isEmpty() && currentBlockSize < SSTableWriter.DATA_BLOCK_SIZE_TARGET) {
                    IteratorEntry entry = pq.peek();
                    Node<String, String> node = entry.getNode();

                    if (lastKey == null || !node.getKey().equals(lastKey)) {
                        lastKey = node.getKey();
                        if (!node.getValue().equals(LSMStore.TOMBSTONE)) {
                            byte[] keyBytes = node.getKey().getBytes(StandardCharsets.UTF_8);
                            byte[] valueBytes = node.getValue().getBytes(StandardCharsets.UTF_8);
                            dos.writeInt(keyBytes.length); dos.write(keyBytes);
                            dos.writeInt(valueBytes.length); dos.write(valueBytes);
                            bloomFilter.put(node.getKey());
                            currentBlockSize += 8 + keyBytes.length + valueBytes.length;
                            lastKeyInBlock = node.getKey();
                            entriesInSSTable++;
                        }
                    }

                    pq.poll();
                    if (entry.getIterator().hasNext()) {
                        entry.setNode(entry.getIterator().next());
                        pq.add(entry);
                    }
                }

                if (lastKeyInBlock != null) {
                    long currentOffset = dos.size();
                    indexs.add(new IndexEntry(lastKeyInBlock, blockStartOffset, (int)(currentOffset - blockStartOffset)));
                }
            }

            if (entriesInSSTable == 0) {
                if(Files.exists(outputPath)){
                    Files.delete(outputPath);
                }
                return false;
            }

            long indexOffset = dos.size();
            dos.writeInt(indexs.size());
            for (IndexEntry indexEntry : indexs) {
                byte[] keyBytes = indexEntry.getLastKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);
                dos.writeLong(indexEntry.getOffset());
                dos.writeInt(indexEntry.getSize());
            }

            long bloomOffset = dos.size();
            bloomFilter.writeTo(dos);
            dos.writeLong(indexOffset);
            dos.writeLong(bloomOffset);
            dos.writeLong(0x123456789ABCDEF0L);

            return true;
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

    public long getNextSSTableId() {
        return nextSSTableId.getAndIncrement();
    }
}

