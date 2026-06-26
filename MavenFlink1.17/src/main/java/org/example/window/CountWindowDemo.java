package org.example.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

public class CountWindowDemo {
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
//        WindowedStream<WaterSensor, String, GlobalWindow> sensorWS = sensorKS.countWindow(5);// 滚动窗口，窗口长度5条数据
        WindowedStream<WaterSensor, String, GlobalWindow> sensorWS = sensorKS.countWindow(5, 2);// 滚动窗口，窗口长度5条数据，滑动步长2条数据。每经过一个步长，都有一个窗口触发输出，第一次输出在第2条数据来的时候
        SingleOutputStreamOperator<String> process = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {
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
                Long maxTs = context.window().maxTimestamp();
                String maxTime = DateFormatUtils.format(maxTs, "yyyy-MM-dd HH:mm:ss");
                long size = elements.spliterator().estimateSize();
                out.collect("key=" + s + "的窗口[最大时间=" + maxTime  + "，包含" + size + "条数据" + elements.toString());
            }
        });
        process.print();
        env.execute();
    }
}
