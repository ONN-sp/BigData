package org.example.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

// 水位超过指定的阈值发送告警，阈值可以动态修改
public class OperatorBroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 数据流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777).map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        // 广播流（广播配置）
        DataStreamSource<String> ConfigDS = env.socketTextStream("localhost", 8888);
        // 1、将广播流广播出去
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcastThreshold", Types.STRING, Types.INT);
        BroadcastStream<String> configBS = ConfigDS.broadcast(broadcastMapState);
        // 2、把数据流和广播流 connect起来
        BroadcastConnectedStream<WaterSensor, String> sensorBCS = sensorDS.connect(configBS);
        // 3、调用process
        SingleOutputStreamOperator<String> process = sensorBCS.process(new BroadcastProcessFunction<WaterSensor, String, String>() {
            /**
             * 数据流的处理方法
             * @param value The stream element.
             * @param ctx A {@link ReadOnlyContext} that allows querying the timestamp of the element,
             *     querying the current processing/event time and updating the broadcast state. The context
             *     is only valid during the invocation of this method, do not store it.
             * @param out The collector to emit resulting elements to
             * @throws Exception
             */
            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 5、通过上下文获取广播状态，取出数据（只读，不能修改）
                Integer threshold = ctx.getBroadcastState(broadcastMapState).get("threshold");
                // 判断广播状态里是否有数据，因为刚启动时，可能是数据流的第一条数据先来
                threshold = threshold==null?0:threshold;
                if (value.getValue() > threshold) {
                    out.collect(value + "，水位超过阈值：" + threshold + "！！！");
                }
            }

            /**
             * 广播流的处理方法
             * @param value The stream element.
             * @param ctx A {@link Context} that allows querying the timestamp of the element, querying the
             *     current processing/event time and updating the broadcast state. The context is only valid
             *     during the invocation of this method, do not store it.
             * @param out The collector to emit resulting elements to
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, String>.Context ctx, Collector<String> out) throws Exception {
                // 4、通过上下文获取广播状态，往里面写数据
                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                broadcastState.put("threshold", Integer.valueOf(value));

            }
        });
        process.print();
        env.execute();
    }

}