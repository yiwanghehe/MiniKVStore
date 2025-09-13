package com.yw.store;

import com.yw.node.Node;
import com.yw.skipList.SkipList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 后台线程，负责执行SSTable的合并操作。
 */
public class CompactionManager extends Thread {
    private final SSTableManager sstManager;
    private volatile boolean shutdown = false;
    private static final int L0_COMPACTION_THRESHOLD = 4; // L0文件数量达到4个时触发合并

    public CompactionManager(SSTableManager sstManager) {
        this.sstManager = sstManager;
        this.setName("compaction-thread");
        this.setDaemon(true); // 设为守护进程
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Thread.sleep(5000); // 五秒检查一次
                runCompaction();
            } catch (InterruptedException e) {
                if (shutdown) break;
            } catch (Exception e) {
                System.out.println("Compaction合并失败: " + e.getMessage());
            }
        }
        System.out.println("Compaction 线程已退出");
    }

    private void runCompaction() throws IOException {
        Map<Integer, ConcurrentSkipListMap<String, SSTableReader>> levels = sstManager.getLevels();
        ConcurrentSkipListMap<String, SSTableReader> l0Files = levels.get(0);

        if(l0Files == null || l0Files.size() < L0_COMPACTION_THRESHOLD) return;

        System.out.println("触发L0 Compaction，文件数: " + l0Files.size());

        //TODO 选择要合并的文件
        List<SSTableReader> filesToCompact = new ArrayList<>(l0Files.values());
        String minKey = filesToCompact.stream().map(SSTableReader::getFirstKey).min(Comparator.naturalOrder()).orElse(null);
        String maxKey = filesToCompact.stream().map(SSTableReader::getLastKey).max(Comparator.naturalOrder()).orElse(null);
        if(minKey == null) return;

        //TODO 选择L1中键范围重叠的文件
        List<SSTableReader> l0OverlapFiles = new ArrayList<>();
        if(levels.containsKey(1)) {
            for (SSTableReader readerL1 : levels.get(1).values()) {
                if(!(readerL1.getFirstKey().compareTo(maxKey) > 0 || readerL1.getLastKey().compareTo(minKey) < 0)){
                    l0OverlapFiles.add(readerL1);
                }
            }
        }
        filesToCompact.addAll(l0OverlapFiles);

        //TODO 执行多路归并排序
        PriorityQueue<IteratorEntry> pq = new PriorityQueue<>();
        for(SSTableReader reader : filesToCompact) {
            SSTableIterator it = reader.iterator();
            if(it.hasNext()) {
                pq.add(new IteratorEntry(it.next(), it));
            }
        }

        //TODO 写入新的SSTable到L1
        String newSstFileName = String.format("1-%d.sst", sstManager.getNextSSTableId());
        Path newSstPath = Paths.get(sstManager.getSstDir(), newSstFileName);

        // 使用一个临时的SkipList来收集合并后的数据，然后一次性写入
        // 生产环境中，使用流式写入，减少内存使用
        SkipList<String, String> mergedData = new SkipList<>();
        String lastKey = null;

        while(!pq.isEmpty()) {
            IteratorEntry entry = pq.poll();
            Node<String, String> node = entry.node;

            // 跳过重复的旧版本key
            if (!node.getKey().equals(lastKey)) {
                // 如果不是删除标记（墓碑），就加入结果集
                if(!node.getValue().equals(LSMStore.TOMBSTONE)) {
                    mergedData.insert(node.getKey(), node.getValue());
                }
                lastKey = node.getKey();
            }

            if (entry.iterator.hasNext()) {
                entry.node = entry.iterator.next();
                pq.add(entry);
            } else {
                entry.iterator.close(); // 关闭此迭代器
            }

        }

        if (mergedData.getNodeCount() > 0) {
            SSTableWriter.write(mergedData, newSstPath.toString());
            SSTableReader newReader = new SSTableReader(newSstPath.toString());
            levels.computeIfAbsent(1, k -> new ConcurrentSkipListMap<>())
                    .put(newSstFileName, newReader);
        }

        //TODO 更新元数据并删除旧文件
        for (SSTableReader reader : filesToCompact) {
            // 从元数据中移除
            String fileName = Paths.get(reader.getFilePath()).getFileName().toString();
            int level = Integer.parseInt(fileName.split("-")[0]);
            levels.get(level).remove(fileName);

            // 关闭文件句柄并删除文件
            reader.close();
            Files.delete(Paths.get(reader.getFilePath()));
        }

        System.out.println("Compaction 完成, 生成新文件: " + newSstFileName);

    }

    public void shutdown() {
        this.shutdown = true;
        this.interrupt();
    }

    private static class IteratorEntry implements Comparable<IteratorEntry> {
        Node<String, String> node;
        SSTableIterator iterator;

        IteratorEntry(Node<String, String> node, SSTableIterator iterator) {
            this.node = node;
            this.iterator = iterator;
        }

        @Override
        public int compareTo(IteratorEntry o){
            return this.node.getKey().compareTo(o.node.getKey());
        }
    }
}
