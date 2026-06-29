package org.example.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.WaterSensor;

public class IntervaloinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 1),
                        Tuple2.of("c", 1)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((timestamp, record) -> timestamp.f1 * 1000L)
                );
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env
                .fromElements(
                        Tuple3.of("a", 1, 1),
                        Tuple3.of("a", 11, 1),
                        Tuple3.of("b", 2, 1),
                        Tuple3.of("b", 12, 1),
                        Tuple3.of("c", 14, 1),
                        Tuple3.of("d", 15, 1)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((record, timestamp) -> record.f1 * 1000L)
                );
        /**
         * Interval Join
         */
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);
        OutputTag<Tuple2<String, Integer>> leftLateData = new OutputTag<>("left late data", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> rightLateData = new OutputTag<>("right late data", Types.TUPLE(Types.STRING, Types.INT, Types.INT));
        // 调用interval join
        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2)
                .between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(leftLateData)
                .sideOutputRightLateData(rightLateData)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    /**
                     * 两条流的数据匹配上，才会调用这个方法
                     * @param left ks1的数据
                     * @param right ks2的数据
                     * @param ctx A context that allows querying the timestamps of the left, right and joined pair.
                     *     In addition, this context allows to emit elements on a side output.
                     * @param out The collector to emit resulting elements to.
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "<======>" + right);
                    }
                });
        process.print();
        process.getSideOutput(leftLateData).printToErr();
        process.getSideOutput(rightLateData).printToErr();
        env.execute();
    }
}
