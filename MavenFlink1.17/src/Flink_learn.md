1. Flink：数据流上的有状态计算引擎，分布式的，可以流批一体处理
   ![img.png](Flink应用.png)
2. Flink是一个开源的、分布式、高性能、高可用的流计算框架，其核心思想就是有状态流计算。在Flink中批处理（有限数据集）是流处理（无限数据集）的一个特例。Flink最强大的是有状态计算（类似动规），比如计算每秒/每分钟的总销售额、一段时间的热门商品排行榜，即要使用之前的状态。Flink和kafka经常结合使用，flink作为kafka的消费者，从指定topic中消费数据，并进行计算，然后返回结果，比如：数据源->kafka->flink（kafka的后端消费者）->结果输出（如数据库、另一个kafka topic等）。注意不是所有情况下用kafka的时候都要用flink，对于有状态计算，即数据处理需要从单条消息处理升级到跨消息（即跨状态）的复杂计算时就可用flink。对于实习中，我都是没有涉及到有状态的计算的，每条消息都是独立的，消费一条消息时不需要知道其它任何消息，因此都是后端服务直接消费kafka，没用flink
3. 流处理：流处理主要针对的是数据流，特点是无界、实时，对系统传输的每个数据依次执行操作，一般用于实时统计。在流处理中，数据被视为无限连续的流，并且会尽快地进行处理。Flink在此模型下可以提供秒级甚至毫秒级的延迟，使其成为需要快速反应和决策的场景（例如实时推荐、欺诈检测等）的理想选
4. 批处理：批处理，也叫作离线处理，一般用于离线统计。这是一种处理存储在系统中的静态数据集的模型。在批处理中，所有数据都被看作是一个有限集合，处理过程通常在非交互式模式下进行，即作业开始时所有数据都已经可用，作业结束时给出所有计算结果。由于批处理允许对整个数据集进行全面分析，因此它适合于需要长期深度分析的场景（如历史数据分析、大规模ETL任务
5. 无界数据流：有定义流的开始，但没有定义流的结束，它们会无休止的产生数据；无界流数据必须持续处理，即数据被摄取后需要立刻处理。不能等到所有数据都到达再处理，因为输入是无限的。比如：kafka等
6. 有界数据流：有定义流的开始，也有定义流的结束；有界流可以在摄取所有数据后再进行计算（批处理），也可以来一条处理一条（流处理）；有界流所有数据可以被排序，所以并不需要有序摄取；有界流处理通常被称为批处理。如：一个文件等
7. Flink 把批处理，当成一种 “有边界、会结束” 的特殊流处理
8. 在 Flink 的视角里，一切数据都可以认为是流，流数据是无界流，而批数据则是有界流
9. 有状态流处理：把流处理需要的额外数据保存成一个“状态”，然后针对这条数据进行处理，并且更新状态
   ![img.png](有状态流处理.png)
10. Flink特点：
     * 高吞吐和低延迟：每秒处理数百万条消息，延迟在毫秒级
     * 结果的准确性：Flink提供了事件时间（事件发生的时间）和处理时间（消息被处理的时间，消息从产生到处理可能会因为传传输而有一定延迟）语义。对于乱序事件流，事件时间语义仍然能提供一致且准确的结果
     * 支持有状态计算，并且支持多种状态内存、 文件、RocksDB
     * 支持高度灵活的窗口（Window） 操作 time、 count、 session
     * 精确一次的状态一致性保证：不丢失、不重复处理消息来保证一致性
     * 可以连接到最常用的存储系统：kafka、hive、jdbc、hdfs、redis等
     * 高可用：本身高可用的设置，加上与K8s、YARN和Mesos的集成，可以实现高可用
11. Hive表是一个底层数据实际存放在HDFS中的逻辑表，使用它后可以使用类sql语句进行访问数据（元数据在 Hive Metastore、数据在 HDFS、用类 SQL 语言定义的‘分布式文件视图’）
12. 一个节点就是一台机器（服务器/PC机）
13. <mark>HDFS：它是一个分布式文件系统，实际上就是对部署在多台独立物理机器上的文件进行管理。HDFS由NameNode和DataNode组成：</mark>
    * NameNode（主管理节点）：整个 HDFS 只有一组（HA：Active+Standby），保存文件系统元数据：文件夹、文件名、文件由哪些 Block 组成、Block 在哪个 DN。不存真实业务数
    * DataNode（数据节点）：集群多台机器全部部署 DN 进程，实际保存 Block 原始数据，数据落地本机磁盘；定时上报块信息给 NN
      ![img.png](HDFS架构.png)
14. 一个NameNode或DataNode就叫一个HDFS节点，多个节点可以组成一个HDFS集群
15. Spark以批处理为根本，微批处理；Flink是流处理
    ![img.png](Flink和Spark对比.png)
16. <mark>Flink VS Spark：</mark>
    * 一般来说，Spark 基于微批处理的方式做同步总有一个“攒批”的过程，所以会有额外开销，因此无法在流处理的低延迟上做到极致
    * 在低延迟流处理场景，Flink 已经有明显的优势。而在海量数据的批处理领域，Spark 能够处理的吞吐量更大
    * Spark Streaming中的流计算其实是微批计算，实时性不如Flink，还有一点很重要的是Spark Streaming不适合有状态的计算，得借助一些存储如：Redis，才能实现。而Flink天然支持有状态的计算
17. Flink应用常见：
    * 电商和市场营销：比如实时数据报表、广告投放、实时推荐（流式计算的实时性）
    * 物联网
    * 物流配送和服务业
    * 银行和金融行业
18. Flink 就是目前全球业界最主流、最标准、最强大的实时处理引擎，因为它是原生流处理，而不是批处理
19. Flink分层API：
    * 底层API：有状态流处理
    * 核心API：DataStream/DataSet API
    * 声明式领域专用语言：Table API
    * 最高层语言：SQL
    ![img.png](FlinkAPI.png)
20. <mark>MySQL大多数情况是基于磁盘的持久化数据库。当使用`Memory`存储引擎时所有数据是存在内存中的，而表结构还是在磁盘中</mark>
21. <mark>所有的Flink程序都可以归纳为三部分构成：Source数据源会源源不断的产生数据，Transformation将产生的数据进行各种业务逻辑的数据处理，最终由Sink输出到外部（console、kafka、redis、DB...）：</mark>
    * `Source`：”源算子“，负责读取数据源
    * `Transformation`：”转换算子“，利用各种算子进行处理加工（比如：keyBy、sum、map等）
    * `Sink`：”下沉算子“，负责数据的输出
22. <mark>所有基于Flink开发的程序都能够映射成一个Dataflows图</mark>
    ![img.png](Dataflows图.png)
    <mark>当Source数据源的数量比较大或计算逻辑相对比较复杂的情况下，需要提高并行度来处理数据，采用并行数据流。通过设置不同算子的并行度，比如Source并行度设置为2，map也是2，此时会启动2个并行的线程来处理数据</mark>
    ![img.png](并行处理.png)
23. DataSet API只能是批处理，它是一口气把所有数据拿到后再处理，而不是来一条处理一条，以WordCut为例，其输出结果为：
    ![img.png](DataSetAPI的WordCut.png)
    而DataStream API是流处理，它是来一条处理一条，以WordCut为例，其输出结果为：
    ![img.png](DataStreamAPI的WordCut.png)（前面的输出编号指的是：并行度，根据自己电脑线程数决定的）
    这个结果也能体现“有状态计算”，在聚合得到3时，2会被Flink自身保存维护，没有用到外部redis等保存
24. 从Flink1.12开始，官方推荐的做法是直接使用`DataStream API`，对于要进行批处理只需要将执行模式设为`BATCH`即可，如`env.setRuntimeMode(RuntimeExecutionMode.BATCH);`
25. DataStream API中必须调用`env.excute()`
26. <mark>java的泛型会在编译阶段进行泛型擦除，如果要使用泛型类型进行链式调用后续方法，需要显示指定泛型类型，否则会报错，比如：</mark>
    ```java
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 读取数据 socket，通过端口模拟无界数据流的输入
    DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777);
    // 处理数据：切分（切分成一个一个单词）、转换（转换为二元组）、分组（一样的单词一组分组）、聚合（一个组内按照个数聚合）
    SingleOutputStreamOperator<Tuple2<String, Integer>> result = socketDS.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                if (!word.trim().isEmpty())
                    out.collect(Tuple2.of(word.trim(), 1));// 使用Collector向下游发送元组
            }
    }
    )
    // 此时如果直接.keyBy()会报错，因为运行时Tuple2<String, Integer>这个类型会被擦除，因此要用returns显示指定出返回类型
    .returns(Types.TUPLE(Types.STRING, Types.INT))// 需要显示指定返回类型，不然用lambda表达式会出现泛型擦除，导致识别不到传入的泛型类型Tuple2<String, Integer>
    .keyBy(value -> value.f0)
    .sum(1);
    // 输出数据
    result.print();
    // 执行任务
    env.execute();
    }
    // Tuple2是个泛型类
    ```
27. Flink是一个典型的Master-Slave架构，架构中包含了两个重要角色，分别是「JobManager」和「TaskManager」。 JobManager相当于是Master，TaskManager相当于是Slave：
    ![img.png](JobManagerTaskManager.png)
    <mark>在Flink中，JobManager负责整个Flink集群任务的调度以及资源的管理。它从客户端中获取提交的应用，然后根据当前Flink集群中TaskManager上TaskSlot的使用情况，为提交的应用分配相应的TaskSlot资源并命令TaskManager启动从客户端中获取的应用。 TaskManager则负责执行作业流的Task，并且缓存和交换数据流。 在TaskManager中资源调度的最小单位是Task slot，默认一个TaskManager有一个Task slot。TaskManager中Task slot的数量表示并发处理Task的数量。
    一台机器节点可以运行多个TaskManager，TaskManager工作期间会向JobManager发送心跳保持连接</mark>
28. <mark>编写的Flink作业代码可以通过`WEB UI`提交(Apache Flink 集群的 Web UI 监控页面)，也可以直接命令行提交`bin/flink run -m [JobManager地址:port] -c [作业的全类名] [jar包路径]`，命令行提交更常用</mark>