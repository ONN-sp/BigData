package org.example.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.bean.WaterSensor;

public class windowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> source = env.socketTextStream("localhost", 7777).map(new RichMapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        KeyedStream<WaterSensor, String> sensorKS = source.keyBy(value -> value.getId());
        // 指定  窗口分配器：指定   用哪一种窗口-----时间 or 计数？滚动、滑动、会话？
        // 1.1 没有keyby的窗口：窗口内的所有数据进入同一个子任务，并行度只能为1
//        source.windowAll();
        // 1.2 有keyby的窗口：每个key上都定义了一组窗口，各自独立地进行统计计算
        // 基于时间的
//        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));// 滚动窗口
//        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));// 滑动窗口
//        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));// 会话窗口
        // 基于计数的
//        sensorKS.countWindow(10);// 滚动窗口
//        sensorKS.countWindow(10, 5);// 滑动窗口
//        sensorKS.window(GlobalWindows.create());// 全局窗口

        // 指定  窗口函数：对窗口内数据的计算逻辑
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));// 滚动窗口
        // 增量聚合：来一条数据，计算一条数据，窗口触发的时候输出计算结果
//        /**
//         * 窗口的reduce：
//         * 1、相同key的第一条数据来的时候，不会调用reduce方法
//         * 2、增量聚合：来一条数据，就会计算一次，但是不会输出
//         * 3、在窗口触发的时候才会输出整个窗口的最终计算结果
//         */
//        SingleOutputStreamOperator<WaterSensor> reduce = sensorWS.reduce(new ReduceFunction<WaterSensor>() {
//            @Override
//            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
//                System.out.println("reduce: " + value1 + " " + value2);
//                return new WaterSensor(value1.getId(), value1.getTimestamp(), value1.getValue() + value2.getValue());
//            }
//        });
//        reduce.print();
        /**
         * 窗口的aggregate
         * 1、属于本窗口的第一条数据来，创建窗口，创建累加器
         * 2、增量聚合：来一条计算一条，调用一次add方法
         * 3、窗口输出时调用一次getResult方法
         * 4、输入、中间累加器、输出类型可以不一样
         */
//        SingleOutputStreamOperator<String> agg = sensorWS.aggregate(
//                /**
//                 * 第一个类型：输入数据的类型
//                 * 第二个类型：累加器的类型，存储中间计算结果的类型
//                 * 第三个类型：输出数据的类型
//                 */
//                new AggregateFunction<WaterSensor, Integer, String>() {
//                    /**
//                     * 创建累加器：初始化累加器
//                     * @return
//                     */
//                    @Override
//                    public Integer createAccumulator() {
//                        System.out.println("createAccumulator");
//                        return 0;
//                    }
//
//                    /**
//                     * 聚合逻辑
//                     */
//                    @Override
//                    public Integer add(WaterSensor value, Integer accumulator) {
//                        System.out.println("add: " + value + " " + accumulator);
//                        return value.getValue() + accumulator;
//                    }
//
//                    /**
//                     * 获取最终结果，窗口触发时输出
//                     * @param accumulator The accumulator of the aggregation
//                     * @return
//                     */
//                    @Override
//                    public String getResult(Integer accumulator) {
//                        System.out.println("getResult: " + accumulator);
//                        return accumulator.toString();
//                    }
//
//                    /**
//                     * 只有会话窗口才会用
//                     * @param a An accumulator to merge
//                     * @param b Another accumulator to merge
//                     * @return
//                     */
//                    @Override
//                    public Integer merge(Integer a, Integer b) {
//                        System.out.println("merge: " + a + " " + b);
//                        return null;
//                    }
//                });
//        agg.print();
        // 全窗口函数：数据来了不计算，存起来，窗口触发的时候，计算并输出结果
//        SingleOutputStreamOperator<String> process = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
//            /**
//             * 全窗口函数：窗口触发时才会调用一次，统一计算
//             * @param s 分组的key
//             * @param context 窗口上下文
//             * @param elements 窗口内的数据
//             * @param out 输出收集器
//             * @throws Exception
//             */
//            @Override
//            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
//                // 上下文可以拿到window对象，测输出流等
//                Long start = context.window().getStart();
//                Long end = context.window().getEnd();
//                String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
//                String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
//                long size = elements.spliterator().estimateSize();
//                out.collect("key=" + s + "的窗口[" + startTime + " , " + endTime + "]包含" + size + "条数据" + elements.toString());
//            }
//        });
//        process.print();
        /**
         * 增量聚合Aggretate+全窗口函数process
         * 增量聚合函数处理数据：来一条计算一条
         * 窗口触发时，增量聚合的结果（只有一条）传递给全窗口函数
         * 经过全窗口函数的处理包装后，输出
         *
         * 结合两者的优点：
         * 1、增量聚合：来一条计算一条，存储中间的计算结果，占用的空间少
         * 2、全窗口函数：可以通过上下文实现灵活的功能
         *
          */

        SingleOutputStreamOperator<String> agg = sensorWS.aggregate(
                new MyAgg(),
                new MyProcess()
        );
        agg.print();
        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor, Integer, String> {
        @Override
        public Integer createAccumulator() {
            System.out.println("createAccumulator");
            return 0;
        }
        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("add: " + value + " " + accumulator);
            return value.getValue() + accumulator;
        }
        @Override
        public String getResult(Integer accumulator) {
            System.out.println("getResult: " + accumulator);
            return accumulator.toString();
        }
        @Override
        public Integer merge(Integer a, Integer b) {
            System.out.println("merge: " + a + " " + b);
            return null;
        }
    }
    public static class MyProcess extends ProcessWindowFunction<String, String, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
            String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
            long size = elements.spliterator().estimateSize();
            out.collect("key=" + s + "的窗口[" + startTime + " , " + endTime + "]包含" + size + "条数据" + elements.toString());
        }
    }
}
