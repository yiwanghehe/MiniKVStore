package com.yw.store;

import com.yw.node.Node;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * 用于Compaction过程，可以遍历一个SSTable中所有的键值对，即遍历数据块。
 */
public class SSTableIterator {
    private final RandomAccessFile raf;
    private final long dataEndOffset; //数据块的终点
    private long currentOffset;

    public SSTableIterator(SSTableReader reader) throws IOException {
        this.raf = new RandomAccessFile(reader.getFilePath(), "r");
        this.currentOffset = 0;
        this.dataEndOffset = reader.getIndexOffset();
    }

    public boolean hasNext() {
        // 当索引偏移量为0时，意味着文件为空或只有footer，没有数据块
        return dataEndOffset > 0 && currentOffset < dataEndOffset;
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
