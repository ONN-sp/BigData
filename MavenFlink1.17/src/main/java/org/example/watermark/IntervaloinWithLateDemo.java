package org.example.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class IntervaloinWithLateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .socketTextStream("localhost", 7777)
                .map(value -> {
                    String[] fields = value.split(",");
                    return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((timestamp, record) -> timestamp.f1 * 1000L)
                );
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env
                .socketTextStream("localhost", 8888)
                .map(value -> {
                    String[] fields = value.split(",");
                    return Tuple3.of(fields[0], Integer.parseInt(fields[1]), Integer.parseInt(fields[2]));
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((record, timestamp) -> record.f1 * 1000L)
                );
        /**
         * Interval Join
         * 1、只支持事件时间
         * 2、指定上下界的偏移，负号表示时间往前
         * 3、主流中只能处理join上的数据
         * 4、和同一条流的多并行度类似，两条流关联后的watermark，以两条流中最小的为准
         * 5、如果有迟到数据（当前数据的事件时间 < 当前watermark），则直接丢弃，主流的process不处理
         *  => 可以在between后，指定将左流或右流的迟到数据放入测输出流
         */
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(r1 -> r1.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(r2 -> r2.f0);
        OutputTag<Tuple2<String, Integer>> leftLateData = new OutputTag<>("left late data", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> rightLateData = new OutputTag<>("right late data", Types.TUPLE(Types.STRING, Types.INT, Types.INT));
        // 调用interval join
        SingleOutputStreamOperator<String> process = ks1.intervalJoin(ks2)
                .between(Time.seconds(-2), Time.seconds(2))
                .sideOutputLeftLateData(leftLateData)// 将ks1的迟到数据，放入测输出流
                .sideOutputRightLateData(rightLateData)// 将ks2的迟到数据，放入测输出流
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
