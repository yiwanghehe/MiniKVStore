package com.yw;

import com.yw.store.LSMStore;

import java.io.IOException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        LSMStore<String, String> store = null;
        try {
            // 启动LSM存储引擎
            store = new LSMStore<>();
            System.out.println("MiniKV 存储引擎启动成功！");
            System.out.println("支持的命令: put key value, get key, exit");

            Scanner scanner = new Scanner(System.in);

            // 主命令循环
            while (true) {
                System.out.print("> ");
                String commandLine = scanner.nextLine();
                if (commandLine == null || commandLine.trim().isEmpty()) {
                    continue;
                }

                String[] parts = commandLine.trim().split("\\s+", 3);
                String command = parts[0].toLowerCase();

                switch (command) {
                    case "put":
                        if (parts.length == 3) {
                            store.put(parts[1], parts[2]);
                            System.out.println("OK");
                        } else {
                            System.out.println("错误: put 命令需要 key 和 value. 用法: put <key> <value>");
                        }
                        break;
                    case "get":
                        if (parts.length == 2) {
                            String value = store.get(parts[1]);
                            if (value != null) {
                                System.out.println(value);
                            } else {
                                System.out.println("(nil)"); // 表示未找到
                            }
                        } else {
                            System.out.println("错误: get 命令需要 key. 用法: get <key>");
                        }
                        break;
                    case "exit":
                        return; // 触发finally块中的store.close()
                    default:
                        System.out.println("未知命令: " + command);
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("存储引擎启动或操作失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (store != null) {
                try {
                    // 确保引擎被优雅关闭
                    store.close();
                } catch (InterruptedException | IOException e) {
                    System.err.println("关闭存储引擎时出错: " + e.getMessage());
                }
            }
        }
    }
}
