package com.afeng.flink.test;

import com.afeng.flink.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
/**
 * @Author xuefeng
 * @Date 2021/6/25 9:51 上午
 */
@Slf4j
public class WorldCountStream {
    public static void main(String[] args) throws Exception {

        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Utils utils = new Utils();
        String path = utils.getResourceFilePath("hello");
        log.warn(path);
        DataStream<String> inputDataStream = env.readTextFile(path);

        // 基于数据流进行转换操作
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WorldCountBatch.MyFlatMapper())
                        .keyBy(e -> e.f0)
                        .sum(1);
        resultStream.print();
        log.warn("执行");
        // 执行
        env.execute();
    }
}
