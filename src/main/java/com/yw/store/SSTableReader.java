package com.yw.store;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 负责读取SSTable文件。
 * (V2: 实现AutoCloseable接口，确保文件句柄能被正确关闭)
 */
public class SSTableReader implements AutoCloseable{
    private final String filePath;
    final RandomAccessFile raf;
    final List<IndexEntry> indexs;
    private final BloomFilter<String> bloomFilter;
    private final String firstKey;
    private final String lastKey;

    public SSTableReader(String filePath) throws IOException {
        this.filePath = filePath;
        this.raf = new RandomAccessFile(filePath, "r");
        this.indexs = new ArrayList<>();

        // 文件长度不足以容纳Footer，说明是空文件或非法文件
        if (raf.length() < 24) {
            this.firstKey = null;
            this.lastKey = null;
            this.bloomFilter = null; // 或者一个空的布隆过滤器
            return;
        }

        // 读取Footer
        long fileLength = raf.length();
        raf.seek(fileLength - 24); // Footer size = 8B index_offset, 8B bloom_offset, 8B magic_number
        long indexOffset = raf.readLong();
        long bloomOffset = raf.readLong();
        long magic = raf.readLong();
        if(magic != 0x123456789ABCDEF0L){
            raf.close(); // 关闭文件句柄
            throw new IOException("非法的SSTable文件: " + filePath);
        }

        // 读取布隆过滤器
        raf.seek(bloomOffset); // 移动到布隆过滤器存储位置
        // 将RandomAccessFile包装成InputStream
        FileChannel channel = raf.getChannel();
        // 重置channel的位置到布隆过滤器开始处
        channel.position(bloomOffset);
        InputStream inputStream = Channels.newInputStream(channel);
        this.bloomFilter = BloomFilter.readFrom(inputStream, Funnels.stringFunnel(StandardCharsets.UTF_8));

        // 读取索引块
        raf.seek(indexOffset);
        int indexsSize = raf.readInt();
        for (int i = 0; i < indexsSize; i++){
            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.read(keyBytes);
            long offset = raf.readLong();
            int size = raf.readInt();
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

    public String get(String key) throws IOException {
        if (bloomFilter == null || indexs.isEmpty()) {
            return null;
        }

        // 检查布隆过滤器
        if (!bloomFilter.mightContain(key)) {
            return null;
        }

        // 二分查找索引块，定位Data Block
        // 注意：binarySearch的比较逻辑是基于lastKey的，所以它会找到第一个lastKey >= key的块
        int blockIndex = Collections.binarySearch(indexs, new IndexEntry(key, 0, 0));
        // blockIndex返回小于0的数表示找不到匹配项，通过公式可以得到key应该在的插入点
        if (blockIndex < 0) {
            blockIndex = -blockIndex - 1;
        }
        // 大于0表示key比所有block的lastKey都要大
        if (blockIndex >= indexs.size()) {
            return null;
        }

        IndexEntry entry = indexs.get(blockIndex);

        // 读取Data Block并查找key
        raf.seek(entry.offset);
        byte[] blockBytes = new byte[entry.size];
        raf.readFully(blockBytes);

        int pos = 0;
        while(pos < blockBytes.length) {
            /**
             * 举例：
             * 字节位置:  pos    pos+1  pos+2  pos+3
             * 字节值:     0x00   0x00   0x01   0x02
             *
             * (blockBytes[pos++] & 0xFF) = (0x00 & 0xFF) = 0x00 = 0
             * (blockBytes[pos++] & 0xFF) = (0x00 & 0xFF) = 0x00 = 0
             * (blockBytes[pos++] & 0xFF) = (0x01 & 0xFF) = 0x01 = 1
             * (blockBytes[pos++] & 0xFF) = (0x02 & 0xFF) = 0x02 = 2
             *
             * 0 << 24 = 0           (第一个字节，最高位)
             * 0 << 16 = 0           (第二个字节)
             * 1 << 8  = 256         (第三个字节)
             * 2 << 0  = 2           (第四个字节，最低位)
             *
             * 0 | 0 | 256 | 2 = 258
             *
             * int keyLen = (0x00 & 0xFF) << 24 |  // 0 << 24 = 0
             *              (0x00 & 0xFF) << 16 |  // 0 << 16 = 0
             *              (0x01 & 0xFF) << 8  |  // 1 << 8  = 256
             *              (0x02 & 0xFF) << 0;    // 2 << 0  = 2
             *                                     // 总计: 0 + 0 + 256 + 2 = 258
             */
            int keyLen = (blockBytes[pos++] & 0xFF) << 24 | (blockBytes[pos++] & 0xFF) << 16 | (blockBytes[pos++] & 0xFF) << 8 | (blockBytes[pos++] & 0xFF);
            String currentKey = new String(blockBytes, pos, keyLen, StandardCharsets.UTF_8);
            // 跳到valLen位置
            pos += keyLen;
            int valLen = (blockBytes[pos++] & 0xFF) << 24 | (blockBytes[pos++] & 0xFF) << 16 | (blockBytes[pos++] & 0xFF) << 8 | (blockBytes[pos++] & 0xFF);
            // 当前[pos, valLen]区间即为value_data区域
            if (currentKey.equals(key)) {
                return new String(blockBytes, pos, valLen, StandardCharsets.UTF_8);
            }
            // 跳到下一个KV的keyLen开头
            pos += valLen;
        }

        return null;
    }

    private String readFirstKey() throws IOException {
        raf.seek(0); // 第一个block的第一个key就是整个文件的firstKey
        int keyLen = raf.readInt();
        byte[] keyBytes = new byte[keyLen];
        raf.readFully(keyBytes);
        return new String(keyBytes, StandardCharsets.UTF_8);
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

    @Override
    public void close() throws IOException {
        raf.close();
    }

    public SSTableIterator iterator() throws IOException {
        return new SSTableIterator(this);
    }

    static class IndexEntry implements Comparable<IndexEntry>{
        String lastKey;
        long offset;
        int size;
        IndexEntry(String lastKey, long offset, int size){
            this.lastKey = lastKey;
            this.offset = offset;
            this.size = size;
        }

        @Override
        public int compareTo(IndexEntry o) {
            // 用lastKey进行比较，来定位key可能所在的块
            return this.lastKey.compareTo(o.lastKey);
        }
    }
}
