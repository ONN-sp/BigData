package org.example.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

import java.time.Duration;

public class WatermakCustomDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200L);
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
                // 周期性水位线生成器
//                .<WaterSensor>forGenerator(ctx -> new MyPeriodWatermarkGenerator<WaterSensor>(3000L))
                // 断点式水位线生成器
                .<WaterSensor>forGenerator(ctx -> new MyPuntuatedWatermarkGenerator<WaterSensor>(3000L))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                        // 返回的时间戳，要毫秒
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        KeyedStream<WaterSensor, String> sensorKS = waterSensorSingleOutputStreamOperator.keyBy(value -> value.getId());
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(10)));// 滚动窗口，使用事件时间语义，watermark才会起作用
        sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
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
        }).print();
        env.execute();
    }
}
