package org.example.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.WaterSensor;

import java.time.Duration;

public class WatermakAllowLatenessDemo {
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
        /**
         * 指定WaterMark策略
         */
        // 定义watermark策略
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                // 乱序watermark，有等待时间
                // 每条数据到达后，当前水印直接更新为这条数据的事件时间。不是来一条数据就生成一条watermark数据，而按周期单独统计区间最大值，全程维护一个全局最大事件时间；每条数据实时更新这个最大值，每隔 200ms 把当前全局最大值封装成水印发送一次
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定时间戳分配器，从数据中提取。每条数据到来都会执行一次
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                    System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                    // 返回的时间戳，要毫秒
                    return element.getTimestamp() * 1000L;
                });
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        KeyedStream<WaterSensor, String> sensorKS = waterSensorSingleOutputStreamOperator.keyBy(value -> value.getId());
        OutputTag<WaterSensor> waterSensorOutputTag = new OutputTag<>("late-data", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<String> process = sensorKS
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))// 滚动窗口，使用事件时间语义，watermark才会起作用
                .allowedLateness(Time.seconds(2))// 推迟2s关窗
                .sideOutputLateData(waterSensorOutputTag)// 关窗后的迟到数据放到测输出流
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /**
                     * 全窗口函数：窗口触发时才会调用一次，统一计算
                     * @param s 分组的key
                     * @param context 窗口上下文
                     * @param elements 窗口内的数据
                     * @param out 输出收集器
                     * @throws Exception
                     */
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        Long start = context.window().getStart();
                        Long end = context.window().getEnd();
                        String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                        String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
                        long size = elements.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口[" + startTime + " , " + endTime + "]包含" + size + "条数据" + elements.toString());
                    }
                });
        process.print();
        process.getSideOutput(waterSensorOutputTag).print("关窗后的迟到数据");// 打印测输出流
        env.execute();
    }
}
/**
 *
 * 乱序：数据的顺序乱了，出现时间小的比时间大的晚来
 * 迟到：当前数据的时间戳 < 当前的watermark
 *
 * 窗口允许迟到
 *  =》推迟关窗时间，在关窗之前，迟到数据来了，还能被窗口计算，来一条吃到数据触发一次计算
 *  =》关窗后，迟到数据不会被计算
 */
