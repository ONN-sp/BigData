// 有界流处理单词计数 基于的DataStream API
package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamingDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);// 基于DataStream API实现流处理，需要将执行模式设为BATCH
        // 读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");
        // 处理数据：切分（切分成一个一个单词）、转换（转换为二元组）、分组（一样的单词一组分组）、聚合（一个组内按照个数聚合）
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    if (!word.trim().isEmpty()) {
                        out.collect(Tuple2.of(word.trim(), 1));// 使用Collector向下游发送元组
                    }
                }
            }
        });
        // 按单词分组
        KeyedStream<Tuple2<String, Integer>, String> tuple2TupleKeyedStream = tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });
        // 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = tuple2TupleKeyedStream.sum(1);
        // 输出数据
        result.print();
        // 执行任务
        env.execute();
    }
}

