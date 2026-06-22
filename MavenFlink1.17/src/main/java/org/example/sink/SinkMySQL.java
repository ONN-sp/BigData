package org.example.sink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.bean.WaterSensor;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("172.17.87.132", 7777).map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        /**
         * 写入mysql
         * 1、只能用老的sink写法：addsink
         * 2、JDBCSink的四个参数：
         *      第一个参数：执行的sql，一般就是insert to等
         *      第二个参数：预编译sql，对占位符填充值
         *      第三个参数：执行选项---》攒批、重试等
         *      第四个参数：连接选项---》url、用户名、密码
         */
        SinkFunction<WaterSensor> mysqlSink = JdbcSink.sink(
                "insert into ws values (?,?,?)",
                // 指定sql中占位符'?'怎么去填充
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement ps, WaterSensor ws) throws SQLException {
                        ps.setString(1, ws.getId());
                        ps.setLong(2, ws.getTimestamp());
                        ps.setInt(3, ws.getValue());
                    }
                },
                JdbcExecutionOptions.builder()
                        // 攒批
                        .withMaxRetries(3)
                        .withBatchSize(100)
                        .withBatchIntervalMs(3000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://172.17.87.132:3306/flink_test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("014779")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()
        );
        source.addSink(mysqlSink);
        env.execute();
    }
}
