package org.example.process;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *  根据id统计每条数据出现次数，每个窗口统一输出访问次数 Top N
 *  增量聚合+全窗口函数结合实现
 */
public class TopNKeyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDs = env.socketTextStream("localhost", 7777).map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        // 指定时间戳分配器，从数据中提取。每条数据到来都会执行一次
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                            // 返回的时间戳，要毫秒
                            return element.getTimestamp() * 1000L;
                        }));
        /**
         * 思路二：使用KeyedprocessFunction实现
         * 1、按照id做keyby，开窗，分别count
         *  =》增量聚合，计算count
         *  =》全窗口，对计算结果count值封装，带上窗口结束时间的标签
         *      =》为了让同一个窗口时间范围内的计算结果到一起去
         * 2、对同一个窗口范围的count值进行处理：排序、取前N个
         *  =》按照windowEnd做keyby
         *  =》使用process，来一条调用一次，需要先存，分开存HashMap，key=windowEnd，value=list
         *      =》使用定时器，对存起来的结果进行排序，取前N个
         *
         */
        // 按照id分组、开创、聚合（增量计算+全量打标签）
        // 开窗聚合后，就算普通的流，没有了窗口信息，需要自己打上窗口的标记windowEnd
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> winAgg = sensorDs.keyBy(value -> value.getId())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new IdCountAgg(), new WindowResult());
        // 按照窗口标签（窗口结束时间）keyby，保证同一个窗口时间范围的结果到一起去，之后再排序取TopN
        winAgg.keyBy(r -> r.f2)
                .process(new TopN(2))
                .print();
        env.execute();
    }
    public static class IdCountAgg implements AggregateFunction<WaterSensor, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }
        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator+1;
        }
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }
        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    /**
     * 利用全窗口函数打标签，因为要获取所有数据才能打标签，所以用全窗口api
     * 泛型如下：
     * 第一个：输入类型=增量函数的输出 count值，Integer
     * 第二个：输出类型=Tuple3<Integer, Integer, Long>，带上窗口结束的标签
     * 第三个：key类型
     * 第四个：窗口类型
     */
    public static class WindowResult extends ProcessWindowFunction<Integer, Tuple3<String, Integer, Long>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Integer> elements, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
            // 迭代器里面只有一条数据，next一次即可
            Integer count = elements.iterator().next();
            long windowEnd = context.window().getEnd();
            out.collect(Tuple3.of(key, count, windowEnd));
        }
    }
    public static class TopN extends KeyedProcessFunction<Long, Tuple3<String, Integer, Long>, String> {
        private HashMap<Long, List<Tuple3<String, Integer, Long>>> dataListMap;
        private int threshold;// 要取的top数量
        public TopN(int threshold) {
            this.threshold = threshold;
            dataListMap = new HashMap<>();
        }
        @Override
        public void processElement(Tuple3<String, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<String, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
            // 来一条处理一次，要排序，需要到齐才行=》同一个窗口存一起
            // 1、存入HashMap
            Long windowEnd = value.f2;
            if(dataListMap.containsKey(windowEnd)){
                // 1.1 存在，直接添加
                dataListMap.get(windowEnd).add(value);
            }else {
                // 1.1 不包含id，是该id的第一条，初始化list
                ArrayList<Tuple3<String, Integer, Long>> datalist = new ArrayList<>();
                datalist.add(value);
                dataListMap.put(windowEnd, datalist);
            }
            // 2、注册一个定时器，windowEnd+1ms
            // 同一个窗口范围，应该同时输出，只不过是一条一条调用processElement方法，如果不用定时器，那么就每一条数据都输出到采集器了，这个和ProcessAllWindowFunction中的process不同
            ctx.timerService().registerEventTimeTimer(windowEnd+1);
        }
        // 3、定时器触发时，取TopN
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = ctx.getCurrentKey();
            List<Tuple3<String, Integer, Long>> list = dataListMap.get(windowEnd);
            list.sort((o1, o2) -> o2.f1 - o1.f1);// 降序
            StringBuilder outStr = new StringBuilder();
            outStr.append("=============");
            for(int i=0;i<Math.min(threshold, list.size());++i) {
                outStr.append("\n");
                outStr.append("Top" + (i+1));
                outStr.append("\n");
                outStr.append("vc=" + list.get(i).f0);
                outStr.append("\n");
                outStr.append("count=" + list.get(i).f1);
                outStr.append("\n");
                outStr.append("窗口结束时间="+ list.get(i).f2);
                outStr.append("\n");
                outStr.append("===============");
            }
            // 用完的list及时清理
            list.clear();
            out.collect(outStr.toString());
        }
    }
}

