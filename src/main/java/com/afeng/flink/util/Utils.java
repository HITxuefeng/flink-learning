package com.afeng.flink.util;

import java.util.Objects;

/**
 * @Author xuefeng
 * @Date 2021/6/25 4:02 下午
 */
public class Utils {
    public String getResourceFilePath(String fileName) {
        String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource("./")).getPath();
        return path + fileName;
    }
}
