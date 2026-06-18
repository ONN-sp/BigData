package org.example.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class fileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 每 2000ms(2秒) 执行一次Checkpoint，语义为 精确一次
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataGeneratorSource<String> dataGenerator = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) {
                        return "Number: " + value;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );
        DataStreamSource<String> datagen = env.fromSource(dataGenerator, WatermarkStrategy.noWatermarks(), "datagen");
        // 输出到文件系统
        FileSink<String> filesink = FileSink.<String>forRowFormat(
                        new Path("E:/FilnkToFile"),
                        new SimpleStringEncoder<>("UTF-8"))
                // 输出文件的一些配置：文件名前缀、后缀
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("bytedance-")// 前缀
                                .withPartSuffix(".log")// 后缀
                                .build()
                )
                // 文件分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 文件滚动策略 10s || 1M
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024 * 1024))
                                .build()
                ).build();
        datagen.sinkTo(filesink);
        env.execute();
    }
}
