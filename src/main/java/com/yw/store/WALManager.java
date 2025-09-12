package com.yw.store;

import com.yw.skipList.SkipList;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * 预写日志（Write-Ahead Log）管理器。
 * 负责将所有写操作在应用到MemTable前记录到磁盘日志中。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public class WALManager<K extends Comparable<K>, V> {
    private static final String WAL_FILE_NAME = "wal.log";
    private final Path walFilePath;
    private BufferedWriter writer;

    public WALManager(String dataDir) throws IOException {
        this.walFilePath = Paths.get(dataDir, WAL_FILE_NAME);
        this.writer = Files.newBufferedWriter(walFilePath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * 记录一个PUT操作。
     *
     * @param key   键
     * @param value 值
     * @throws IOException 写入失败
     */
    public synchronized void logPut(K key, V value) throws IOException {
        // 简化格式: "PUT,key,value"
        this.writer.write("PUT," + key.toString() + "," + value.toString());
        this.writer.newLine();
        this.writer.flush(); // 强制刷盘，确保数据持久化
    }

    /**
     * 关闭日志写入器。
     */
    public void close() throws IOException {
        if (this.writer != null) {
            this.writer.close();
            System.out.println("WAL已关闭");
        }
    }

    /**
     * 轮转日志文件。通常在MemTable切换后调用。
     * 旧的WAL可以被归档或删除（当对应MemTable成功刷盘后）。
     */
    public synchronized void rotateLog() throws IOException {
        this.close();
        // 将旧日志文件重命名为带时间戳的归档文件
        if (Files.exists(walFilePath)) {
            Path archivedPath = Paths.get(walFilePath.toString() + "." + System.currentTimeMillis());
            Files.move(walFilePath, archivedPath);
        }
        // 创建新的WAL文件
        this.writer = Files.newBufferedWriter(walFilePath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * 从WAL日志文件中恢复数据到MemTable。
     *
     * @param memTable 要恢复数据的内存表
     * @throws IOException 读取失败
     */
    public void recover(SkipList<K, V> memTable) throws IOException {
        if (!Files.exists(walFilePath)) {
            return; // 日志不存在，无需恢复
        }

        List<String> lines = Files.readAllLines(walFilePath, StandardCharsets.UTF_8);
        for (String line : lines) {
            String[] parts = line.split(",", 3);
            if (parts.length == 3 && "PUT".equals(parts[0])) {
                // 假设Key和Value都是String类型，这里需要更通用的反序列化
                K key = (K) parts[1];
                V value = (V) parts[2];
                memTable.insert(key, value);
            }
        }
    }
}
