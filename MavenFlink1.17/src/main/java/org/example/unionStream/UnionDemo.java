package org.example.unionStream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);
      DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33, 44, 55);
      DataStreamSource<String> source3 = env.fromElements("111", "222", "333", "444", "555");
//      DataStream<Integer> union = source1.union(source2).union(source3.map(value -> Integer.parseInt(value)));
        DataStream<Integer> union = source1.union(source2, source3.map(value -> Integer.parseInt(value)));
        union.print();
      env.execute();
    }
}
