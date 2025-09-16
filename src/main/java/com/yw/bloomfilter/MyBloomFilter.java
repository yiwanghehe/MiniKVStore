package com.yw.bloomfilter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;

/**
 * 自定义的布隆过滤器实现。
 */
public class MyBloomFilter {

    private final int numHashFunctions; // 哈希函数的个数 k
    private final BitSet bitset; // 底层的位数组
    private final int bitSetSize; // 位数组的大小 m

    /**
     * 构造一个布隆过滤器。
     * @param expectedInsertions 预期的插入元素数量 n
     * @param fpp 期望的误判率 p
     */
    private MyBloomFilter(long expectedInsertions, double fpp) {
        if (expectedInsertions <= 0) {
            expectedInsertions = 1; // 避免除以零
        }
        // 根据公式计算最优的位数组大小 m 和哈希函数个数 k
        this.bitSetSize = optimalM(expectedInsertions, fpp);
        this.numHashFunctions = optimalK(expectedInsertions, bitSetSize);
        this.bitset = new BitSet(bitSetSize);
    }

    private MyBloomFilter(int numHashFunctions, BitSet bitset, int bitSetSize) {
        this.numHashFunctions = numHashFunctions;
        this.bitset = bitset;
        this.bitSetSize = bitSetSize;
    }

    /**
     * 工厂方法创建布隆过滤器实例
     * @param expectedInsertions 预期的插入元素数量 n
     * @param fpp 期望的误判率 p
     * @return MyBloomFilter 实例
     */
    public static MyBloomFilter create(long expectedInsertions, double fpp) {
        return new MyBloomFilter(expectedInsertions, fpp);
    }

    /**
     * 向布隆过滤器中添加一个元素。
     */
    public void put(String key) {
        int hash1 = Hashing.murmur3_32(key, 0);
        int hash2 = Hashing.murmur3_32(key, hash1); // 使用第一个哈希作为第二个的种子

        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            if (combinedHash < 0) {
                combinedHash = ~combinedHash; // Ensure positive index
            }
            bitset.set(combinedHash % bitSetSize);
        }
    }

    /**
     * 检查一个元素是否可能存在于布隆过滤器中。
     * @return 如果元素绝对不存在，返回 false；如果元素可能存在，返回 true。
     */
    public boolean mightContain(String key) {
        int hash1 = Hashing.murmur3_32(key, 0);
        int hash2 = Hashing.murmur3_32(key, hash1);

        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            if (!bitset.get(combinedHash % bitSetSize)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 将布隆过滤器序列化到输出流。
     * 写入顺序为：
     * 1. 哈希函数个数K (4字节 Int)
     * 2. 位数组的大小 (4字节 Int)
     * 3. 位数组字节数据的长度 (4字节 Int)
     * 4. 位数组的实际字节数据
     */
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(numHashFunctions);
        out.writeInt(bitSetSize);
        byte[] bytes = bitset.toByteArray();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    /**
     * 从输入流反序列化布隆过滤器。
     */
    public static MyBloomFilter readFrom(InputStream in) throws IOException {
        DataInputStream dis = new DataInputStream(in);
        int numHashFunctions = dis.readInt();
        int bitSetSize = dis.readInt();
        int byteArrayLength = dis.readInt();
        byte[] bytes = new byte[byteArrayLength];
        dis.readFully(bytes);
        BitSet bitset = BitSet.valueOf(bytes);
        return new MyBloomFilter(numHashFunctions, bitset, bitSetSize);
    }

    // --- Helper methods for calculating optimal parameters ---

    // 计算最优的位数组大小 m
    private static int optimalM(long n, double p) {
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    // 计算最优的哈希函数数量 k
    private static int optimalK(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }
}

