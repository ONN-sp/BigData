package org.example.Source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class kafakSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从kafka读取数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("172.17.87.132:9092")// 指定kafka节点的地址和端口
                .setGroupId("example")// 指定消费者组的id
                .setTopics("topic_1")// 指定要订阅的topic
                .setValueOnlyDeserializer(new SimpleStringSchema())// 指定反序列化器，这里使用SimpleStringSchema
                .setStartingOffsets(OffsetsInitializer.latest())// 指定从最新的offset开始读取  从最早开始读取，OffsetsInitializer默认是earliest
                .build();
        DataStreamSource<String> kafkasource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource");
        kafkasource.print();
        env.execute();
    }
}
