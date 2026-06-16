package org.example.Transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

public class flatmapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        waterSensorDataStreamSource.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if("s1".equals(value.getId())){
                    // 一进一出
                    out.collect(value.getValue().toString());
                }else if("s2".equals(value.getId())){
                    // 一进二出
                    out.collect(value.getTimestamp().toString());
                    out.collect(value.getValue().toString());
                }
            }
        }).print();
        env.execute();
    }
}
