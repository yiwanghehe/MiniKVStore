package com.yw.store;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.yw.node.Node;
import com.yw.skipList.SkipList;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 负责将MemTable内容写入到新的SSTable文件格式中。
 * SSTable 格式:
 * [Data Block 1]
 * [Data Block 2]
 * ...
 * [Index Block]
 * [Bloom Filter]
 * [Footer] (8B index_offset, 8B bloom_offset, 8B magic_number)
 */

/**
 * [Data Block]
 * ├── [Entry 1]
 * │   ├── key_length (4B int)
 * │   ├── key_data (variable)
 * │   ├── value_length (4B int)
 * │   └── value_data (variable)
 * ├── [Entry 2]
 * │   ├── key_length (4B int)
 * │   ├── key_data (variable)
 * │   ├── value_length (4B int)
 * │   └── value_data (variable)
 * └── ...
 *
 * [Index Block]
 * ├── index_count (4B int)
 * ├── [Index Entry 1]
 * │   ├── last_key_length (4B int)
 * │   ├── last_key_data (variable)
 * │   ├── block_offset (8B long)
 * │   └── block_size (4B int)
 * ├── [Index Entry 2]
 * │   ├── last_key_length (4B int)
 * │   ├── last_key_data (variable)
 * │   ├── block_offset (8B long)
 * │   └── block_size (4B int)
 * └── ...
 *
 * [Bloom Filter] 具体格式由Guava库定义，包含位数组和哈希函数信息
 *
 * [Footer]
 * ├── index_offset (8B long)
 * ├── bloom_offset (8B long)
 * └── magic_number (8B long = 0x123456789ABCDEF0L)
 */
public class SSTableWriter {
    private static final long MAGIC_NUMBER = 0x123456789ABCDEF0L;
    private static final int DATA_BLOCK_SIZE_TARGET = 4 * 1024; // 4KB

    public static void write(SkipList<String, String> memTable, String filePath) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath);
             DataOutputStream dos = new DataOutputStream(fos)) {

            List<IndexEntry> indexs = new ArrayList<>();
            BloomFilter<String> bloomFilter = BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8),
                    memTable.getNodeCount(),
                    0.01 // 1%误判率
            );

            // 写入数据块
            Node<String, String> current = memTable.getHeader().forwards.get(0);
            long currentOffset = 0;

            while(current != null){
                long blockStartOffset = currentOffset;
                List<Node<String, String>> blockEntries = new ArrayList<>();
                int currentBlockSize = 0;
                String lastKeyInBlock = null;

                // 填充一个数据块
                while(current != null && currentBlockSize < DATA_BLOCK_SIZE_TARGET) {
                    blockEntries.add(current);
                    bloomFilter.put(current.getKey());
                    currentBlockSize = current.getKey().length() + current.getValue().length();
                    lastKeyInBlock = current.getKey();
                    current = current.getForwards().get(0);
                }

                // 将数据块写入文件
                for(Node<String, String> node : blockEntries){
                    byte[] keyBytes = node.getKey().getBytes(StandardCharsets.UTF_8);
                    byte[] valueBytes = node.getValue().getBytes(StandardCharsets.UTF_8);
                    dos.writeInt(keyBytes.length);
                    dos.write(keyBytes);
                    dos.writeInt(valueBytes.length);
                    dos.write(valueBytes);
                }
                currentOffset = dos.size();
                indexs.add(new IndexEntry(lastKeyInBlock, blockStartOffset, (int)(currentOffset - blockStartOffset)));
            }

            // 写入索引块
            long indexOffset = dos.size();
            dos.writeInt(indexs.size());
            for (IndexEntry entry : indexs){
                byte[] keyBytes = entry.lastKey.getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);
                dos.writeLong(entry.offset);
                dos.writeInt(entry.size);
            }

            // 写入布隆过滤器
            long bloomOffset = dos.size();
            bloomFilter.writeTo(dos);

            // 写入Footer
            dos.writeLong(indexOffset);
            dos.writeLong(bloomOffset);
            dos.writeLong(MAGIC_NUMBER);
        }
    }

    // 内部类，用于表示索引条目
    private static class IndexEntry {
        String lastKey;
        long offset;
        int size;
        IndexEntry(String lastKey, long offset, int size){
            this.lastKey = lastKey;
            this.offset = offset;
            this.size = size;
        }
    }
}
