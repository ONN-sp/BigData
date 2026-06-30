package org.example.process;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.WaterSensor;

import java.text.DateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *  根据id统计出现的次数的前TopN
 */
public class TopNDemo {
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
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                // 指定时间戳分配器，从数据中提取。每条数据到来都会执行一次
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                    System.out.println("数据=" + element + ",recordTs=" + recordTimestamp);
                    // 返回的时间戳，要毫秒
                    return element.getTimestamp() * 1000L;
                });
        SingleOutputStreamOperator<WaterSensor> sensorDs = source.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);
        /**
         * 思路一：所有数据放到一起，用hashmap存，key=vc，value=count值
         * 最近10秒=窗口长度；每5秒输出=滑动步长
         */
        sensorDs.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))// 返回AllWindowedStream
                .process(new MyTopNPAWF())
                .print();
        env.execute();
    }
    public static class MyTopNPAWF extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            // 定义一个hashmap来存，key=vc，value=count
            HashMap<String, Integer> vcCountMap = new HashMap<>();
            // 1、遍历数据，统计各个id出现的次数
            for(WaterSensor element:elements) {
                String id = element.getId();
                if (vcCountMap.containsKey(id)) {
                    // 1.1 如果vc在hashmap中，count++
                    vcCountMap.put(id, vcCountMap.get(element.getId()) + 1);
                }else {
                    // 1.2 如果vc不在hashmap中，count=1
                    vcCountMap.put(id, 1);
                }
            }
            // 2、对count值进行排序
            ArrayList<Tuple2<String, Integer>> datas = new ArrayList<>();
            // 遍历hashmap赋值给list
            for (String s : vcCountMap.keySet())
                datas.add(Tuple2.of(s, vcCountMap.get(s)));
            datas.sort((o1, o2) -> o2.f1 - o1.f1);// 降序
            // 3、取出count最大的2个id
            StringBuilder outStr = new StringBuilder();
            outStr.append("===============");
            for(int i=0;i<Math.min(2, datas.size());++i) {
                outStr.append("Top" + (i+1));
                outStr.append("\n");
                outStr.append("vc=" + datas.get(i).f0);
                outStr.append("\n");
                outStr.append("count=" + datas.get(i).f1);
                outStr.append("\n");
                outStr.append("窗口结束时间="+ DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS"));
                outStr.append("\n");
                outStr.append("===============");
            }
            out.collect(outStr.toString());
        }
    }
}

