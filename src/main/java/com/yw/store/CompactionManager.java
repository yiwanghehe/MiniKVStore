package com.yw.store;

/**
 * 后台线程，负责执行SSTable的合并操作。
 */
public class CompactionManager extends Thread {
    private final SSTableManager sstManager;
    private volatile boolean shutdown = false;

    public CompactionManager(SSTableManager sstManager) {
        this.sstManager = sstManager;
        this.setName("compaction-thread");
        this.setDaemon(true);
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Thread.sleep(5000);
                sstManager.compact();
            } catch (InterruptedException e) {
                if (shutdown) break;
            } catch (Exception e) {
                System.out.println("Compaction合并失败: " + e.getMessage());
                e.printStackTrace();
            }
        }

        try {
            System.out.println("Compaction 线程正在执行最后一次清理...");
            sstManager.compact();
        } catch(Exception e) {
            System.err.println("Compaction 最终清理失败: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Compaction 线程已退出");
    }

    public void shutdown() {
        this.shutdown = true;
        this.interrupt();
    }


}

