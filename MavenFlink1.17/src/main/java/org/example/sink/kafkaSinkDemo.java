package org.example.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class kafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<String> localhost = env.socketTextStream("172.17.87.132", 7777);
        /**
         * 注意：如果要使用 精准一次 写入kafka，需要满足以下条件：
         * 1、开启chekcpoint
         * 2、设置事务前缀
         * 3、设置事务超时时间：checkpoint间隔时间 < 超时时间 < 最大的15分钟
         */
        KafkaSink<String> kkSink = KafkaSink.<String>builder()
                .setBootstrapServers("172.17.87.132:9092")
                // 指定发送方序列化器，topic名称，具体的序列化方式
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("ws")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                // 写到kafka的一致性级别：精准一次，至少一次。如果是精确一次，必须设置事务的前缀
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)// 精准一次
                .setTransactionalIdPrefix("bytedance-")
                // 如果是精准一次，必须设置事务的超时时间，需要大于checkpoint间隔
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10*60*1000+"")
                .build();
        localhost.print("socket-data>>>");
        localhost.sinkTo(kkSink);
        env.execute();
    }
}
