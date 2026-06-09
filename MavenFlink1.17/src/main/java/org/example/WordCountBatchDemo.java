// 批处理单词计数 基于的DataSet API，下面这种方法已经过时了，不会用了
package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取文件
        DataSource<String> textStream = env.readTextFile("input/word.txt");
        // 核心逻辑：分词 + 计数
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = textStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                // 按照空格分词
                String[] words = value.split(" ");
                // 将单词转换为（单词，1）元组
                for (String word : words) {
                    if (!word.trim().isEmpty())
                        out.collect(Tuple2.of(word.trim(), 1));// 使用Collector向下游发送元组
                }
            }
        });
        // 按单词分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupby = wordAndOne.groupBy(0);
        // 各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupby.sum(1);// 1是位置，表示第二个原生
        sum.print();
    }
}
