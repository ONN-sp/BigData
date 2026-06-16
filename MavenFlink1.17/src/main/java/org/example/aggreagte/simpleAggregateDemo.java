package org.example.aggreagte;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.WaterSensor;

public class simpleAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s11", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        // 按照key来分组
        /**
         * keyBy返回的是一个KeyedStream，键控流
         * keyBy不是转换算子，只是对数据进行重分区，不能设置并行度
         * keyBu分组与分区的区别：
         *  1）keyBy是对数据分组，保证相同key的数据在同一个分区
         *  2）分区：一个子任务，可以理解为一个分区
         */
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorDataStreamSource.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        // 传位置索引的适用于Tuple类型,POJO不行
        waterSensorStringKeyedStream.sum("value").print();
        env.execute();
    }
}
