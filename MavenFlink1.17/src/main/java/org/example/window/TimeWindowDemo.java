package org.example.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

public class TimeWindowDemo {
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
        KeyedStream<WaterSensor, String> sensorKS = source.keyBy(value -> value.getId());
        WindowedStream<WaterSensor, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));// 滚动窗口
//        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));// 滑动窗口 长度10s 步长5s
//        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));// 会话窗口，会话时间5s
//        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
//            @Override
//            public long extract(WaterSensor element) {
//                // 从数据中提取timestamp，然后以它作为当前会话的超时时间
//                return element.getTimestamp() * 1000L;
//            }
//        }));// 会话窗口，以每条数据的timestamp为动态会话超时时间
        SingleOutputStreamOperator<String> process = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
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
        env.execute();
    }
}
/**
 * 以时间类型的滚动窗口为例，分析原理：
 * 窗口什么时候触发  输出？
 *         时间进展 >= 窗口的最大时间戳（end-1ms）
 * 窗口是怎么划分的：窗口开始时间不是直接取当前窗口第一条数据来的时间
 *         start = 向下取整，取窗口长度的整数倍
 *         end = start + 窗口长度
 *         窗口是左闭右开的
 * 窗口的生命周期：
 *         窗口创建：属于本窗口的第一条数据到达时，才创建窗口，放入一个单例的集合中
 *         窗口触发：当进展 >= 窗口的最大时间戳（end-1ms）时，触发窗口
 *         窗口关闭：时间进展 >= 窗口的最大时间戳（end-1ms）+ 允许迟到时间（默认0） 时，关闭窗口。关闭窗口和触发窗口默认是同时的
 */
