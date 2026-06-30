package org.example.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.WaterSensor;

import java.time.Duration;

public class SideOutputDemo {
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
        SingleOutputStreamOperator<WaterSensor> SensorDS = source.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<WaterSensor> process = SensorDS.keyBy(sensor -> sensor.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                // 使用测输出流告警
                                if (value.getValue() > 10)
                                    ctx.output(warnTag, "当前水位=" + value.getValue() + ",大于阈值10!!!");
                                // 主流正常 发送数据
                                out.collect(value);
                            }
                        }
                );
        process.print();
        process.getSideOutput(warnTag).printToErr();
        env.execute();
    }
}