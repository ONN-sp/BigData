package org.example.unionStream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 合并两条流，根据id字段进行匹配
 */
public class ConnectKeyByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c"),
                Tuple2.of(4, "d"),
                Tuple2.of(5, "e")
                );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb1", 1),
                Tuple3.of(3, "cc1", 1),
                Tuple3.of(4, "dd1", 1),
                Tuple3.of(5, "ee1", 1)
        );
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2).keyBy(s1 -> s1.f0, s2 -> s2.f0);
        /**
         * 实现互相匹配的效果：
         * 1、两条流不一定谁的数据先来
         * 2、每条流，有数据来就先存到一个变量中
         *      hashmap
         *      =>key=id，第一个字段值
         *      =>value=List<数据><
         * 3、每条流有数据来的时候，除了存变量中，不知道对方是否有匹配的数据，要去另一条流存到变量查找是否有匹配上的数据
         */
        SingleOutputStreamOperator<Object> process = connect.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Object>() {
            // 定义hashmap用来存数据
            Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

            /**
             * 第一条流的处理逻辑
             * @param value The stream element
             * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
             *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
             *     timers and querying the time. The context is only valid during the invocation of this
             *     method, do not store it.
             * @param out The collector to emit resulting elements to
             * @throws Exception
             */
            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                Integer id = value.f0;
                // s1的数据来录就存到变量中
                if (!s1Cache.containsKey(id)) {
                    List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                    s1Values.add(value);
                    s1Cache.put(id, s1Values);
                } else {
                    // key存在，不是该key的第一条数据，直接添加到s1Cache中
                    s1Cache.get(id).add(value);
                }
                // 去s2Cache中查找是否有id能匹配上的，匹配就输出，没有就不输出
                if (s2Cache.containsKey(id)) {
                    List<Tuple3<Integer, String, Integer>> s2Values = s2Cache.get(id);
                    for (Tuple3<Integer, String, Integer> s2Value : s2Values) {
                        out.collect("s1:" + value + "<=======>" + "s2:" + s2Value);
                    }
                }
            }

            /**
             * 第二条流的处理逻辑
             * @param value The stream element
             * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
             *     {@link TimeDomain} of the firing timer and getting a {@link TimerService} for registering
             *     timers and querying the time. The context is only valid during the invocation of this
             *     method, do not store it.
             * @param out The collector to emit resulting elements to
             * @throws Exception
             */
            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                Integer id = value.f0;
                // s2的数据来录就存到变量中
                if (!s2Cache.containsKey(id)) {
                    List<Tuple3<Integer, String, Integer>> s2Values = new ArrayList<>();
                    s2Values.add(value);
                    s2Cache.put(id, s2Values);
                } else {
                    // key存在，不是该key的第一条数据，直接添加到s1Cache中
                    s2Cache.get(id).add(value);
                }
                // 去s1Cache中查找是否有id能匹配上的，匹配就输出，没有就不输出
                if (s1Cache.containsKey(id)) {
                    List<Tuple2<Integer, String>> s1Values = s1Cache.get(id);
                    for (Tuple2<Integer, String> s1Value : s1Values) {
                        out.collect("s1:" + s1Value + "<=======>" + "s2:" + value);
                    }
                }
            }
        });
        process.print();
        env.execute();
    }
}
