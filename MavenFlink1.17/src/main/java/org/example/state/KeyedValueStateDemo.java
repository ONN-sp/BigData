package org.example.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

import java.time.Duration;

// 连续两个相邻水位值差超过10就告警
public class KeyedValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("localhost", 7777).map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                // 乱序watermark，有等待时间
                // 每条数据到达后，当前水印直接更新为这条数据的事件时间。不是来一条数据就生成一条watermark数据，而按周期单独统计区间最大值，全程维护一个全局最大事件时间；每条数据实时更新这个最大值，每隔 200ms 把当前全局最大值封装成水印发送一次
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定时间戳分配器，从数据中提取。每条数据到来都会执行一次
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                    // 返回的时间戳，要毫秒
                    return element.getTimestamp() * 1000L;
                });
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        KeyedStream<WaterSensor, String> sensorKS = waterSensorSingleOutputStreamOperator.keyBy(value -> value.getId());
        sensorKS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    ValueState<Integer> lastValueState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态
                        // 状态描述器两个参数：第一个参数：起个名字，唯一不重复；第二个参数：存储的类型
                        lastValueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastValue", Integer.class));
                    }
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        // 取出上一条的水位值
                        int lastValue = lastValueState.value() == null ? 0 : lastValueState.value();// 取出值状态里的数据
                        // 求差值的绝对值，判断是否超过10
                        if (Math.abs(lastValue - value.getValue()) > 10) {
                            out.collect("传感器=" + value.getId() + "，当前水位值=" + value.getValue() + "，与上一条水位值=" + lastValue + "，相差超过10！！！");
                        }
                        // 保存更新自己的水位值
                        lastValueState.update(value.getValue());
                    }
                }
        ).print();
        env.execute();
    }
}
