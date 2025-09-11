package com.yw;

import com.yw.skipList.SkipList;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        SkipList<String, String> skipList = new SkipList<>();
        Scanner scanner = new Scanner(System.in);

        label:
        while (true) {
            String command = scanner.nextLine();
            String[] commandList = command.split(" ");
            switch (commandList[0]) {
                case "insert": {
                    boolean b = skipList.insert(commandList[1], commandList[2]);
                    if (b) {
                        System.out.println("Key: " + commandList[1] + " Value: " + commandList[2] + " 插入成功!");
                    } else {
                        System.out.println("Key: " + commandList[1] + " Value: " + commandList[2] + " 插入失败");
                    }
                    break;
                }
                case "delete": {
                    boolean b = skipList.delete(commandList[1]);
                    if (b) {
                        System.out.println("Key: " + commandList[1] + " 被删除!");
                    } else {
                        System.out.println("不存在此key: " + commandList[1]);
                    }
                    break;
                }
                case "search": {
                    boolean b = skipList.search(commandList[1]);
                    if (b) {
                        System.out.println("Key: " + commandList[1] + " 被找到!");
                    } else {
                        System.out.println("Key: " + commandList[1] + " 不存在!");
                    }
                    break;
                }
                case "get":
                    if (!skipList.search(commandList[1])) {
                        System.out.println("Key: " + commandList[1] + " 不存在!");
                    }
                    String value = skipList.get(commandList[1]);
                    if (value != null) {
                        System.out.println("Key: " + commandList[1] + " " + "Value: " + value);
                    }
                    break;
                case "dump":
                    skipList.dump();
                    System.out.println("已保存.");
                    break;
                case "load":
                    skipList.load(String::new, String::new);
                    break;
                case "exit":
                    break label;
                default:
                    System.out.println("********skiplist*********");
                    skipList.display();
                    System.out.println("*************************");
                    break;
            }
        }
    }
}
