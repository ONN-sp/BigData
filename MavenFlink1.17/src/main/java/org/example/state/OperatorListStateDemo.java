package org.example.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 在map算子中计算数据的个数
public class OperatorListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.socketTextStream("localhost", 7777)
                .map(new MyCountMapFunction())
                .print();


        env.execute();
    }
    // 实现CheckpointedFunction接口
    public static class MyCountMapFunction implements MapFunction<String, Long>, CheckpointedFunction {
        private  Long count = 0L;
        private ListState<Long> listState;
        @Override
        public Long map(String s) throws Exception {
            count++;
            return count;
        }

        /**
         * 本地变量持久化：将本地变量拷贝到算子状态中
         * 开启checkpoint时，触发一次 checkpoint 快照时，当前算子所有子任务都会执行一次snapshotState方法
         * @param context the context for drawing a snapshot of the operator
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState");
            // 清空算子状态
            listState.clear();
            // 把本地变量拷贝到算子状态中
            listState.add(count);
        }

        /**
         * 初始化本地变量：程序恢复时，从状态中，把数据添加到本地变量中，每个子任务调用一次
         * @param context the context for initializing the operator
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState");
            // 初始化算子状态
            listState = context.getOperatorStateStore()
                               .getListState(new ListStateDescriptor<>("count", Types.LONG));
//                               .getUnionListState(new ListStateDescriptor<>("unionCount", Types.LONG));
            // 从算子状态中，把数据添加到本地变量中
            if (context.isRestored()) {// 恢复成功时
                Iterable<Long> longs = listState.get();
                for (Long l : longs) {
                    count += l;
                }
            }
        }
    }
}
/**
 * 算子状态中，list和unionlist的区别：并行度改变时，怎么重新分配状态
 * 1、list状态：轮询均分给新的并行子任务
 * 2、unionlist状态：原先的多个子任务的状态，合并成一份完整的，会把完整的列表广播给新的并行子任务，每个子任务都拿到一份完整的状态
 */
