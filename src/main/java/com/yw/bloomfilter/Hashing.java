package com.yw.bloomfilter;

import java.nio.charset.StandardCharsets;

/**
 * 提供高性能哈希函数的工具类。
 * 使用MurmurHash3算法
 */
public final class Hashing {

    /**
     * 对给定的字符串使用 Murmur3 算法计算 32 位哈希值。
     * @param data 要哈希的字符串
     * @param seed 哈希种子
     * @return 32位哈希值
     */
    public static int murmur3_32(String data, int seed) {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        return murmur3_32(bytes, bytes.length, seed);
    }

    /**
     * Murmur3 32-bit 哈希函数的核心实现。
     * 开源的标准实现。
     */
    public static int murmur3_32(byte[] data, int len, int seed) {
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        int h1 = seed;
        int roundedEnd = (len & 0xfffffffc);

        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        int k1 = 0;
        switch (len & 0x03) {
            case 3:
                k1 = (data[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k1 |= (data[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k1 |= (data[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }

        h1 ^= len;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }
}
