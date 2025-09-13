package com.yw.store;

import com.yw.node.Node;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * 用于Compaction过程，可以遍历一个SSTable中所有的键值对。
 */
public class SSTableIterator {
    private final RandomAccessFile raf;
    private final long fileEndOffset; // 数据块的终点
    private long currentOffset;

    public SSTableIterator(SSTableReader reader) throws IOException {
        this.raf = new RandomAccessFile(reader.getFilePath(), "r");
        this.currentOffset = 0;

        // 检查SSTable是否为空。如果索引为空，则没有数据块可迭代。
        if (reader.indexs.isEmpty()){
            this.fileEndOffset = 0; // 没有数据，迭代范围为0
        } else {
            // 如果不为空，迭代的终点就是索引块的起始位置。
            this.fileEndOffset = reader.getIndexOffset();
        }
    }

    public boolean hasNext() {
        return currentOffset < fileEndOffset;
    }

    public Node<String, String> next() throws IOException {
        if (!hasNext()) return null;

        raf.seek(currentOffset);

        int keyLen = raf.readInt();
        byte[] keyBytes = new byte[keyLen];
        raf.readFully(keyBytes);

        int valLen = raf.readInt();
        byte[] valBytes = new byte[valLen];
        raf.readFully(valBytes);

        currentOffset = raf.getFilePointer();

        return new Node<>(new String(keyBytes, StandardCharsets.UTF_8), new String(valBytes, StandardCharsets.UTF_8), 0);
    }

    public void close() throws IOException {
        raf.close();
    }

}
