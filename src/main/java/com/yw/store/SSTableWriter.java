package com.yw.store;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.yw.node.Node;
import com.yw.skipList.SkipList;
import com.yw.store.entry.IndexEntry;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 负责将MemTable内容写入到新的SSTable文件格式中。
 */
public class SSTableWriter {
    private static final long MAGIC_NUMBER = 0x123456789ABCDEF0L;
    public static final int DATA_BLOCK_SIZE_TARGET = 4 * 1024; // 4KB

    public static void write(SkipList<String, String> memTable, String filePath) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath);
             DataOutputStream dos = new DataOutputStream(fos)) {

            List<IndexEntry> indexs = new ArrayList<>();
            BloomFilter<String> bloomFilter = BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8),
                    memTable.getNodeCount(),
                    0.01 // 1%误判率
            );

            Node<String, String> current = memTable.getHeader().forwards.get(0);
            long currentOffset = 0;

            while(current != null){
                long blockStartOffset = currentOffset;
                String lastKeyInBlock = null;
                int currentBlockSize = 0;

                // 使用一个临时列表来缓存一个块的内容，然后一次性写入
                List<Node<String, String>> blockEntries = new ArrayList<>();
                while(current != null && currentBlockSize < DATA_BLOCK_SIZE_TARGET) {
                    blockEntries.add(current);
                    bloomFilter.put(current.getKey());
                    currentBlockSize += 8 + current.getKey().length() + current.getValue().length();
                    lastKeyInBlock = current.getKey();
                    current = current.getForwards().get(0);
                }

                // 写入数据块
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
                byte[] keyBytes = entry.getLastKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);
                dos.writeLong(entry.getOffset());
                dos.writeInt(entry.getSize());
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

}

