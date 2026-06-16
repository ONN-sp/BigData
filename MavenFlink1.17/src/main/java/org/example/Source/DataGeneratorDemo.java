package org.example.Source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

public class DataGeneratorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * 数据生成器Source，四个参数：
         * 第一个：GeneratorFunction，用于生成数据的函数，返回值类型为T，需要自己实现，输入类型固定是Long
         * 第二个：Long类型，自动生成的数字序列最大值，达到这个值就停止
         * 第三个：RateLimiterStrategy，指定数据生成器的速率限制策略，比如每秒生成几条数据
         * 第四个：TypeInformation，指定数据生成器的输出类型信息，默认是Object.class
         */
        DataGeneratorSource<String> dataGenerator = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) {
                        return "Number: " + value;
                    }
                },
                10,
                RateLimiterStrategy.perSecond(1),
                Types.STRING
        );
        DataStreamSource<String> datagen = env.fromSource(dataGenerator, WatermarkStrategy.noWatermarks(), "datagen");
        datagen.print();
        env.execute();
    }
}
