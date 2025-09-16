package com.yw.store;

import com.google.common.cache.Cache;
import com.yw.bloomfilter.MyBloomFilter;
import com.yw.store.entry.IndexEntry;

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
import java.util.concurrent.ExecutionException;

/**
 * 负责读取SSTable文件
 */
public class SSTableReader implements AutoCloseable {
    private final String filePath;
    final List<IndexEntry> indexs;
    private final MyBloomFilter bloomFilter;
    private final String firstKey;
    private final String lastKey;
    private final long indexOffset; //索引块的起始偏移量

    private final Cache<String, byte[]> blockCache; // 引入共享的数据块缓存

    public SSTableReader(String filePath, Cache<String, byte[]> blockCache) throws IOException {
        this.filePath = filePath;
        this.blockCache = blockCache;
        this.indexs = new ArrayList<>();

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
            this.indexOffset = indexOffsetValue;
            long bloomOffset = tempRaf.readLong();
            long magic = tempRaf.readLong();
            if (magic != 0x123456789ABCDEF0L) {
                throw new IOException("非法的SSTable文件: " + filePath);
            }

            // 读取布隆过滤器
            tempRaf.seek(bloomOffset);
            FileChannel channel = tempRaf.getChannel();
            channel.position(bloomOffset);
            InputStream inputStream = Channels.newInputStream(channel);
            this.bloomFilter = MyBloomFilter.readFrom(inputStream);

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

            // 获得firstKey和lastKey
            if (!indexs.isEmpty()) {
                this.lastKey = indexs.get(indexs.size() - 1).getLastKey();
                this.firstKey = readFirstKey();
            } else {
                this.firstKey = null;
                this.lastKey = null;
            }
        }
    }

    public String get(String key) throws IOException {
        if (bloomFilter == null || !bloomFilter.mightContain(key)) {
            return null;
        }

        // 二分查找
        int blockIndex = Collections.binarySearch(indexs, new IndexEntry(key, 0, 0));
        if (blockIndex < 0) {
            blockIndex = -blockIndex - 1;
        }
        if (blockIndex >= indexs.size()) {
            return null;
        }

        IndexEntry entry = indexs.get(blockIndex);
        byte[] blockBytes;

        // 加入缓存机制
        try {
            String cacheKey = filePath + ":" + entry.getOffset();
            blockBytes = blockCache.get(cacheKey, () -> {
                try (RandomAccessFile raf = new RandomAccessFile(this.filePath, "r")) {
                    raf.seek(entry.getOffset());
                    byte[] _blockBytes = new byte[entry.getSize()];
                    raf.readFully(_blockBytes);
                    return _blockBytes;
                }
            });

        } catch (ExecutionException e) {
            throw new IOException("从缓存加载数据块失败", e);
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
        try (RandomAccessFile raf = new RandomAccessFile(this.filePath, "r")) {
            raf.seek(0);
            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            return new String(keyBytes, StandardCharsets.UTF_8);
        }
    }

    /**
     * SSTable被删除时清理其在缓存中的数据
     */
    public void invalidateCache() {
        for (IndexEntry entry : indexs) {
            String cacheKey = filePath + ":" + entry.getOffset();
            blockCache.invalidate(cacheKey);
        }
    }

    public String getFilePath() { return this.filePath; }
    public String getFirstKey() { return this.firstKey; }
    public String getLastKey() { return this.lastKey; }
    public long getIndexOffset() { return this.indexOffset; }

    @Override
    public void close() {}

    public SSTableIterator iterator() throws IOException {
        return new SSTableIterator(this);
    }

}

