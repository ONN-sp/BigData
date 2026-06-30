package org.example.process;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

import java.time.Duration;

public class keyedProcessTimerDemo {
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
                    System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                    // 返回的时间戳，要毫秒
                    return element.getTimestamp() * 1000L;
                });
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        KeyedStream<WaterSensor, String> sensorKS = waterSensorSingleOutputStreamOperator.keyBy(value -> value.getId());
        // Process:keyed
        SingleOutputStreamOperator<String> process = sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            /**
             * 来一条数据调用一次
             * @param value The input value.
             * @param ctx A {@link Context} that allows querying the timestamp of the element and getting a
             *     {@link TimerService} for registering timers and querying the time. The context is only
             *     valid during the invocation of this method, do not store it.
             * @param out The collector for returning result values.
             * @throws Exception
             */
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                TimerService timerService = ctx.timerService();// 定时器
                String currentKey = ctx.getCurrentKey();
                // 注册定时器：事件时间
//                Long ts = ctx.timestamp();// 数据中提取出来的事件时间
//                timerService.registerEventTimeTimer(5000L);
//                System.out.println("当前事件时间=" + ts + "，注册了一个5s的定时器");
                // 注册定时器：处理时间
                long currentTs = timerService.currentProcessingTime();
                timerService.registerProcessingTimeTimer(currentTs+5000L);
                System.out.println("当前key=" + currentKey + "当前处理时间=" + currentTs + "，注册了一个5s后的定时器");
                // 获取当前process的watermark
//                long wm = timerService.currentWatermark();// 当前水印，
//                System.out.println("当前数据=" + value + "，当前水印=" + wm);
            }

            /**
             * 时间进展到到定时器时间，调用该方法
             * @param timestamp The timestamp of the firing timer.
             * @param ctx An {@link OnTimerContext} that allows querying the timestamp, the {@link
             *     TimeDomain}, and the key of the firing timer and getting a {@link TimerService} for
             *     registering timers and querying the time. The context is only valid during the invocation
             *     of this method, do not store it.
             * @param out The collector for returning result values.
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                String currentKey = ctx.getCurrentKey();
                System.out.println("当前key=" + currentKey + "现在时间是" + timestamp + "定时器触发");
            }
        });
        env.execute();
    }
}
/**
 * 定时器总结：
 * 1、keyed才有
 * 2、事件时间定时器，通过watermark来触发的
 *    watermark >= 注册时间
 *    注意：watermark = 当前最大事件时间-等待时间-1ms
 * 3、在process中获取当前watermark，显示的是上一次的watermark
 *   =》因为process还没接收到这个条数据对应生成的watermark
 */
