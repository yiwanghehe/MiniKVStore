package com.yw.store;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 负责读取SSTable文件。
 * (V4: 缓存indexOffset元数据以供Iterator使用)
 */
public class SSTableReader implements AutoCloseable {
    private final String filePath;
    final List<IndexEntry> indexs;
    private final BloomFilter<String> bloomFilter;
    private final String firstKey;
    private final String lastKey;
    private final long indexOffset; // 新增：缓存索引块的起始偏移量

    public SSTableReader(String filePath) throws IOException {
        this.filePath = filePath;
        this.indexs = new ArrayList<>();

        // 使用 try-with-resources 确保文件句柄在构造函数中被正确关闭
        try (RandomAccessFile tempRaf = new RandomAccessFile(filePath, "r")) {
            // 文件长度不足以容纳Footer，说明是空文件或非法文件
            if (tempRaf.length() < 24) {
                this.firstKey = null;
                this.lastKey = null;
                this.bloomFilter = null;
                this.indexOffset = 0;
                return;
            }

            // 读取Footer
            long fileLength = tempRaf.length();
            tempRaf.seek(fileLength - 24); // Footer size = 8B index_offset, 8B bloom_offset, 8B magic_number
            long indexOffsetValue = tempRaf.readLong();
            this.indexOffset = indexOffsetValue; // 缓存索引块偏移量
            long bloomOffset = tempRaf.readLong();
            long magic = tempRaf.readLong();
            if (magic != 0x123456789ABCDEF0L) {
                throw new IOException("非法的SSTable文件: " + filePath);
            }

            // 读取布隆过滤器
            tempRaf.seek(bloomOffset); // 移动到布隆过滤器存储位置
            FileChannel channel = tempRaf.getChannel();
            channel.position(bloomOffset);
            InputStream inputStream = Channels.newInputStream(channel);
            this.bloomFilter = BloomFilter.readFrom(inputStream, Funnels.stringFunnel(StandardCharsets.UTF_8));

            // 读取索引块
            tempRaf.seek(indexOffsetValue);
            int indexsSize = tempRaf.readInt();
            for (int i = 0; i < indexsSize; i++) {
                int keyLen = tempRaf.readInt();
                byte[] keyBytes = new byte[keyLen];
                tempRaf.readFully(keyBytes);
                long offset = tempRaf.readLong();
                int size = tempRaf.readInt();
                indexs.add(new IndexEntry(new String(keyBytes, StandardCharsets.UTF_8), offset, size));
            }

            // 获取第一个和最后一个key
            if (!indexs.isEmpty()) {
                this.lastKey = indexs.get(indexs.size() - 1).lastKey;
                this.firstKey = readFirstKey();
            } else {
                this.firstKey = null;
                this.lastKey = null;
            }
        }
    }

    // 移除了 synchronized，现在可以被多线程安全地并发调用
    public String get(String key) throws IOException {
        if (bloomFilter == null || indexs.isEmpty()) {
            return null;
        }
        if (!bloomFilter.mightContain(key)) {
            return null;
        }

        int blockIndex = Collections.binarySearch(indexs, new IndexEntry(key, 0, 0));
        if (blockIndex < 0) {
            blockIndex = -blockIndex - 1;
        }
        if (blockIndex >= indexs.size()) {
            return null;
        }

        IndexEntry entry = indexs.get(blockIndex);
        byte[] blockBytes;

        // 为本次读操作创建独立的、短暂的文件句柄
        try (RandomAccessFile raf = new RandomAccessFile(this.filePath, "r")) {
            raf.seek(entry.offset);
            blockBytes = new byte[entry.size];
            raf.readFully(blockBytes);
        }

        ByteBuffer buffer = ByteBuffer.wrap(blockBytes);
        while (buffer.hasRemaining()) {
            if (buffer.remaining() < 4) break;
            int keyLen = buffer.getInt();
            if (buffer.remaining() < keyLen) break;
            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
            String currentKey = new String(keyBytes, StandardCharsets.UTF_8);

            if (buffer.remaining() < 4) break;
            int valLen = buffer.getInt();
            if (buffer.remaining() < valLen) break;

            if (currentKey.equals(key)) {
                byte[] valBytes = new byte[valLen];
                buffer.get(valBytes);
                return new String(valBytes, StandardCharsets.UTF_8);
            } else {
                buffer.position(buffer.position() + valLen);
            }
        }
        return null;
    }

    private String readFirstKey() throws IOException {
        // 同样使用独立的句柄来读取
        try (RandomAccessFile raf = new RandomAccessFile(this.filePath, "r")) {
            raf.seek(0);
            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            return new String(keyBytes, StandardCharsets.UTF_8);
        }
    }

    public String getFilePath() {
        return this.filePath;
    }

    public String getFirstKey() {
        return this.firstKey;
    }

    public String getLastKey() {
        return this.lastKey;
    }

    // 新增：为Iterator提供元数据
    public long getIndexOffset() {
        return this.indexOffset;
    }

    @Override
    public void close() {
        // 因为没有共享的raf实例，所以这里什么都不用做
    }

    public SSTableIterator iterator() throws IOException {
        // SSTableIterator 在内部会创建自己的 RandomAccessFile，所以它是安全的
        return new SSTableIterator(this);
    }

    // 内部类，保持不变
    static class IndexEntry implements Comparable<IndexEntry> {
        String lastKey;
        long offset;
        int size;
        IndexEntry(String lastKey, long offset, int size) {
            this.lastKey = lastKey;
            this.offset = offset;
            this.size = size;
        }
        @Override
        public int compareTo(IndexEntry o) {
            return this.lastKey.compareTo(o.lastKey);
        }
    }
}

