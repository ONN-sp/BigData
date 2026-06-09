// 无界流处理单词计数 基于的DataStream API
package org.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamUnboundDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取数据 socket，通过端口模拟无界数据流的输入
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        // 处理数据：切分（切分成一个一个单词）、转换（转换为二元组）、分组（一样的单词一组分组）、聚合（一个组内按照个数聚合）
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = socketDS.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                String[] words = value.split(" ");
                for (String word : words) {
                    if (!word.trim().isEmpty())
                        out.collect(Tuple2.of(word.trim(), 1));// 使用Collector向下游发送元组
                }
        }
        )
        .returns(Types.TUPLE(Types.STRING, Types.INT))// 需要显示指定返回类型，不然用lambda表达式会出现泛型擦除，导致识别不到传入的泛型类型Tuple2<String, Integer>
        .keyBy(value -> value.f0)
        .sum(1);
        // 输出数据
        result.print();
        // 执行任务
        env.execute();
    }
}
