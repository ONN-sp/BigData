package org.example.Transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);

        integerDataStreamSource.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                RuntimeContext runtimeContext = getRuntimeContext();
                int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks();
                System.out.println("子任务编号="+indexOfThisSubtask+"启动，子任务名称="+taskNameWithSubtasks+"，调用open()");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("子任务编号="+getRuntimeContext().getIndexOfThisSubtask()+"启动，子任务名称="+getRuntimeContext().getTaskNameWithSubtasks()+"，close()");
            }

            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }
        }).print();
        env.execute();
    }
}
