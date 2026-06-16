package org.example.partition;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
        // 随机分区
//        socketDS.shuffle().print();
        // 轮询分区
//        socketDS.rebalance().print();
        // 缩放轮询分区
//        socketDS.rescale().print();
        // 广播：发送给下游所有的子任务
//        socketDS.broadcast().print();
        // global：全局分区，将所有数据发送给下游算子的第一个并行子任务中，即强行让下游子任务并行度为1
//        socketDS.global().print();
        // oen-to-one分区
//        socketDS.forward().print();
        // keyBy分区
//        socketDS.keyBy(new KeySelector<String, String>() {
//            @Override
//            public String getKey(String value) throws Exception {
//                return value;
//
//            }
//        }).print();

        env.execute();
    }
}
