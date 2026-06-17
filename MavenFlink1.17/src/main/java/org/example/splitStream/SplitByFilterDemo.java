package org.example.splitStream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分流： 奇数流、偶数流
 */
public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        /**
         * 使用filter实现分流
         * 缺点：同一个数据要被处理两遍
         */
        socketDS.filter(value -> Integer.parseInt(value) % 2 == 0).print("偶数流");
        socketDS.filter(value -> Integer.parseInt(value) % 2 != 0).print("奇数流");

        env.execute();
    }
}
