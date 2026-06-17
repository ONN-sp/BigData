package org.example.splitStream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.bean.WaterSensor;

public class sideOutputDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("localhost", 7777).map(value -> {
            String[] datas = value.split(",");
            return new WaterSensor(datas[0], Long.parseLong(datas[1]), Integer.parseInt(datas[2]));
        });
        /**
         * 使用测输出流实现分流
         * 通过process得到的结果是主流的数据
         * 总结步骤：
         *  1、使用process算子
         *  2、定义outputTag对象
         *  3、调用ctx.output
         *  4、通过主流获取测流
         */
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.getId();
                if ("s1".equals(id)) {
                    // 如果是s1，放到测输出流s1中
                    ctx.output(s1Tag, value);
                } else if ("s2".equals(id)) {
                    // 如果是s2，放到测输出流s2中
                    ctx.output(s2Tag, value);
                } else {
                    // 如果是s3，放到主流中
                    out.collect(value);
                }
            }
        });
        process.print("主流");
        // 打印测输出流
        process.getSideOutput(s1Tag).print("s1");
        process.getSideOutput(s2Tag).print("s2");
        env.execute();
    }
}
