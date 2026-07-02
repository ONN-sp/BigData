// 无界流处理单词计数 基于的DataStream API
package org.example.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CheckConfigDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 1、启用检查点，周期5s，默认是barrier对齐精准一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2、指定检查点的存储位置
        checkpointConfig.setCheckpointStorage("hdfs://localhost:8020/chk");
        // 3、超时时间、最小间隔时间，默认10min
        checkpointConfig.setCheckpointTimeout(60000);
        // 4、同时运行中的checkpoint的最大数量，默认是1个，很多时候没有设置
        checkpointConfig.setMaxConcurrentCheckpoints(2);
        // 5、最小等待间隔，上一轮checkpoint结束到下一轮checkpoint开始的间隔，设置了>0，并发就会变成1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        // 6、取消作业时，checkpoint的数据保留在外部系统
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);// 作业取消后就不保留检查点数据
        // 7、允许checkpoint连续失败的次数，默认0，表示checkpoint一失败，job就挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        // 读取数据 socket，通过端口模拟无界数据流的输入
        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777).setParallelism(1);
        // 处理数据：切分（切分成一个一个单词）、转换（转换为二元组）、分组（一样的单词一组分组）、聚合（一个组内按照个数聚合）
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = socketDS.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                String[] words = value.split(" ");
                for (String word : words) {
                    if (!word.trim().isEmpty())
                        out.collect(Tuple2.of(word.trim(), 1));// 使用Collector向下游发送元组
                }
        }
        )
        .returns(Types.TUPLE(Types.STRING, Types.INT))// 需要显示指定返回类型，不然用lambda表达式会出现泛型擦除，导致识别不到传入的泛型类型Tuple2<String, Integer>
        .keyBy(value -> value.f0)
        .sum(1);
        // 输出数据
        result.print();
        // 执行任务
        env.execute();
    }
}
