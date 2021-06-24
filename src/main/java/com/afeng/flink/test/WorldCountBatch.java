package com.afeng.flink.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author xuefeng
 * @Date 2021/6/24 6:00 下午
 */
public class WorldCountBatch {
    public static void main(String[] args) throws Exception{
        // 创建执行环节
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读入数据
        DataSet<String> inputDataSet = env.readTextFile("/Users/xuefeng/self_github/flink-learning/src/main/resources/hello");

        DataSet<Tuple2<String, Integer>> resultDataSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0) // 按照第一个位置 word 分组
                .sum(1); // 将第二个位置上的数值求和
        resultDataSet.print();
    }

    private static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = value.split(" ");
            for(String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
