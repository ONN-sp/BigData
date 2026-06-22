package org.example.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class SinkKafkaWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<String> localhost = env.socketTextStream("172.17.87.132", 7777);
        /**
         * 如果要指定写入kafka的key
         * 可以自定义反序列化器也可以直接用.setValueSerializationSchema(new SimpleStringSchema())、.setKeySerializationSchema(new SimpleStringSchema())
         * 1、实现一个接口，重写序列化方法
         * 2、指定key，转成字节数组
         * 3、指定value，转成字节数组
         * 4、返回一个ProducerRecord对象，把key、value放进去
         */
        KafkaSink<String> kkSink = KafkaSink.<String>builder()
                .setBootstrapServers("172.17.87.132:9092")
                // 指定发送方序列化器，topic名称，具体的序列化方式
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<String>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(String element, KafkaSinkContext context, Long timestamp) {
                                String[] fields = element.split(",");
                                byte[] key = fields[0].getBytes(StandardCharsets.UTF_8);// key
                                byte[] value = element.getBytes(StandardCharsets.UTF_8);
                                return new ProducerRecord<>("ws", key, value);
                            }
                        }
//                        KafkaRecordSerializationSchema.<String>builder()
//                                .setTopic("ws")
//                                .setValueSerializationSchema(new SimpleStringSchema())
//                                .setKeySerializationSchema(new SimpleStringSchema())
//                                .build()
                )
                // 写到kafka的一致性级别：精准一次，至少一次。如果是精确一次，必须设置事务的前缀
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)// 精准一次
                .setTransactionalIdPrefix("bytedance-")
                // 如果是精准一次，必须设置事务的超时时间
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000+"")
                .build();
        localhost.print("socket-data>>>");
        localhost.sinkTo(kkSink);
        env.execute();
    }
}
