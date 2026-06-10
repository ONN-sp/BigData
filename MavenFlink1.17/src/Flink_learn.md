1. 国内大数据底层架构都是基于Hadoop（由HDFS、YARN、MR三部分组成）
2. Flink：数据流上的有状态计算引擎，分布式的，可以流批一体处理
   ![img.png](Flink应用.png)
3. Flink是一个开源的、分布式、高性能、高可用的流计算框架，其核心思想就是有状态流计算。在Flink中批处理（有限数据集）是流处理（无限数据集）的一个特例。Flink最强大的是有状态计算（类似动规），比如计算每秒/每分钟的总销售额、一段时间的热门商品排行榜，即要使用之前的状态。Flink和kafka经常结合使用，flink作为kafka的消费者，从指定topic中消费数据，并进行计算，然后返回结果，比如：数据源->kafka->flink（kafka的后端消费者）->结果输出（如数据库、另一个kafka topic等）。注意不是所有情况下用kafka的时候都要用flink，对于有状态计算，即数据处理需要从单条消息处理升级到跨消息（即跨状态）的复杂计算时就可用flink。对于实习中，我都是没有涉及到有状态的计算的，每条消息都是独立的，消费一条消息时不需要知道其它任何消息，因此都是后端服务直接消费kafka，没用flink
4. 流处理：流处理主要针对的是数据流，特点是无界、实时，对系统传输的每个数据依次执行操作，一般用于实时统计。在流处理中，数据被视为无限连续的流，并且会尽快地进行处理。Flink在此模型下可以提供秒级甚至毫秒级的延迟，使其成为需要快速反应和决策的场景（例如实时推荐、欺诈检测等）的理想选
5. 批处理：批处理，也叫作离线处理，一般用于离线统计。这是一种处理存储在系统中的静态数据集的模型。在批处理中，所有数据都被看作是一个有限集合，处理过程通常在非交互式模式下进行，即作业开始时所有数据都已经可用，作业结束时给出所有计算结果。由于批处理允许对整个数据集进行全面分析，因此它适合于需要长期深度分析的场景（如历史数据分析、大规模ETL任务
6. 无界数据流：有定义流的开始，但没有定义流的结束，它们会无休止的产生数据；无界流数据必须持续处理，即数据被摄取后需要立刻处理。不能等到所有数据都到达再处理，因为输入是无限的。比如：kafka等
7. 有界数据流：有定义流的开始，也有定义流的结束；有界流可以在摄取所有数据后再进行计算（批处理），也可以来一条处理一条（流处理）；有界流所有数据可以被排序，所以并不需要有序摄取；有界流处理通常被称为批处理。如：一个文件等
8. Flink 把批处理，当成一种 “有边界、会结束” 的特殊流处理
9. 在 Flink 的视角里，一切数据都可以认为是流，流数据是无界流，而批数据则是有界流
10. 有状态流处理：把流处理需要的额外数据保存成一个“状态”，然后针对这条数据进行处理，并且更新状态
    ![img.png](有状态流处理.png)
11. Flink特点：
     * 高吞吐和低延迟：每秒处理数百万条消息，延迟在毫秒级
     * 结果的准确性：Flink提供了事件时间（事件发生的时间）和处理时间（消息被处理的时间，消息从产生到处理可能会因为传传输而有一定延迟）语义。对于乱序事件流，事件时间语义仍然能提供一致且准确的结果
     * 支持有状态计算，并且支持多种状态内存、 文件、RocksDB
     * 支持高度灵活的窗口（Window） 操作 time、 count、 session
     * 精确一次的状态一致性保证：不丢失、不重复处理消息来保证一致性
     * 可以连接到最常用的存储系统：kafka、hive、jdbc、hdfs、redis等
     * 高可用：本身高可用的设置，加上与K8s、YARN和Mesos的集成，可以实现高可用
12. Hive表是一个底层数据实际存放在HDFS中的逻辑表，使用它后可以使用类sql语句进行访问数据（元数据在 Hive Metastore、数据在 HDFS、用类 SQL 语言定义的‘分布式文件视图’）
13. 一个节点就是一台机器（服务器/PC机）
14. <mark>HDFS：它是一个分布式文件系统，实际上就是对部署在多台独立物理机器上的文件进行管理。HDFS由NameNode和DataNode组成：</mark>
    * NameNode（主管理节点）：整个 HDFS 只有一组（HA：Active+Standby），保存文件系统元数据：文件夹、文件名、文件由哪些 Block 组成、Block 在哪个 DN。不存真实业务数
    * DataNode（数据节点）：集群多台机器全部部署 DN 进程，实际保存 Block 原始数据，数据落地本机磁盘；定时上报块信息给 NN
      ![img.png](HDFS架构.png)
15. 一个NameNode或DataNode就叫一个HDFS节点，多个节点可以组成一个HDFS集群
16. Spark以批处理为根本，微批处理；Flink是流处理
    ![img.png](Flink和Spark对比.png)
17. <mark>Flink VS Spark：</mark>
    * 一般来说，Spark 基于微批处理的方式做同步总有一个“攒批”的过程，所以会有额外开销，因此无法在流处理的低延迟上做到极致
    * 在低延迟流处理场景，Flink 已经有明显的优势。而在海量数据的批处理领域，Spark 能够处理的吞吐量更大
    * Spark Streaming中的流计算其实是微批计算，实时性不如Flink，还有一点很重要的是Spark Streaming不适合有状态的计算，得借助一些存储如：Redis，才能实现。而Flink天然支持有状态的计算
18. Flink应用常见：
    * 电商和市场营销：比如实时数据报表、广告投放、实时推荐（流式计算的实时性）
    * 物联网
    * 物流配送和服务业
    * 银行和金融行业
19. Flink 就是目前全球业界最主流、最标准、最强大的实时处理引擎，因为它是原生流处理，而不是批处理
20. Flink分层API：
    * 底层API：有状态流处理
    * 核心API：DataStream/DataSet API
    * 声明式领域专用语言：Table API
    * 最高层语言：SQL
    ![img.png](FlinkAPI.png)
21. <mark>MySQL大多数情况是基于磁盘的持久化数据库。当使用`Memory`存储引擎时所有数据是存在内存中的，而表结构还是在磁盘中</mark>
22. <mark>所有的Flink程序都可以归纳为三部分构成：Source数据源会源源不断的产生数据，Transformation将产生的数据进行各种业务逻辑的数据处理，最终由Sink输出到外部（console、kafka、redis、DB...）：</mark>
    * `Source`：”源算子“，负责读取数据源
    * `Transformation`：”转换算子“，利用各种算子进行处理加工（比如：keyBy、sum、map等）
    * `Sink`：”下沉算子“，负责数据的输出
23. <mark>所有基于Flink开发的程序都能够映射成一个Dataflows图</mark>
    ![img.png](Dataflows图.png)
    <mark>当Source数据源的数量比较大或计算逻辑相对比较复杂的情况下，需要提高并行度来处理数据，采用并行数据流。通过设置不同算子的并行度，比如Source并行度设置为2，map也是2，此时会启动2个并行的线程来处理数据</mark>
    ![img.png](并行处理.png)
24. DataSet API只能是批处理，它是一口气把所有数据拿到后再处理，而不是来一条处理一条，以WordCut为例，其输出结果为：
    ![img.png](DataSetAPI的WordCut.png)
    而DataStream API是流处理，它是来一条处理一条，以WordCut为例，其输出结果为：
    ![img.png](DataStreamAPI的WordCut.png)（前面的输出编号指的是：并行度，根据自己电脑线程数决定的）
    这个结果也能体现“有状态计算”，在聚合得到3时，2会被Flink自身保存维护，没有用到外部redis等保存
25. 从Flink1.12开始，官方推荐的做法是直接使用`DataStream API`，对于要进行批处理只需要将执行模式设为`BATCH`即可，如`env.setRuntimeMode(RuntimeExecutionMode.BATCH);`
26. DataStream API中必须调用`env.excute()`
27. <mark>java的泛型会在编译阶段进行泛型擦除，如果要使用泛型类型进行链式调用后续方法，需要显示指定泛型类型，否则会报错，比如：</mark>
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
28. Flink是一个典型的Master-Slave架构，架构中包含了两个重要角色，分别是「JobManager」和「TaskManager」。 JobManager相当于是Master，TaskManager相当于是Slave：
    ![img.png](JobManagerTaskManager.png)
    <mark>在Flink中，JobManager负责整个Flink集群任务的调度以及资源的管理。它从客户端中获取提交的应用，然后根据当前Flink集群中TaskManager上TaskSlot的使用情况，为提交的应用分配相应的TaskSlot资源并命令TaskManager启动从客户端中获取的应用。 TaskManager则负责执行作业流的Task，并且缓存和交换数据流。 在TaskManager中资源调度的最小单位是Task slot，默认一个TaskManager有一个Task slot。TaskManager中Task slot的数量表示并发处理Task的数量。
    一台机器节点可以运行多个TaskManager，TaskManager工作期间会向JobManager发送心跳保持连接</mark>
29. <mark>编写的Flink作业代码可以通过`WEB UI`(这个UI是Flink JobManager 的 Web UI)提交(Apache Flink 集群的 Web UI 监控页面)，也可以直接命令行提交`bin/flink run -m [JobManager地址:port] -c [作业的全类名] [jar包路径]`，命令行提交更常用</mark>
30. 可以在`Apache Flink`上查看当前部署的集群，此时可以`localhost:8081`或者`172.17.87.132:8081`访问Flink集群的WEB UI监控页面：
    ![img.png](WEBUI.png)
    在wsl中启动nc，然后提交作业到WEB UI，查看其日志：
    ![img.png](WEBUI日志.png)
31. WSL和Windows是不同的操作系统，Flink集群部署到wsl中，那么就要在wsl中打开`nc -l -p 7777`，而不是在windows中打开，虽然此时可以通过windows的WEB UI看到WSL中部署的flink集群，那是因为：Windows 是它的 “宿主机”，两者之间网络是通的，像同一台电脑上的两个系统，
你在 WSL2 里启动一个服务，监听 0.0.0.0:8081（Flink 就是这样，flink-conf.yaml就配置了`rest.bind-address: 0.0.0.0`，表示监听所有网卡、所有 IP）。 WSL2 会自动把这个端口 “映射” 到 Windows 的 localhost 上。 所以：Windows 浏览器访问 localhost:8081，或者访问WSL的IP 172.17.87.132:8081都能直接连到WSL里的Flink JobManager。
但是不能写成127.0.0.1，就只能 WSL 内部访问，Windows 就打不开了
32. Flink为各种场景提供了不同的部署模式：
    * 会话模式：需要提前启动一个集群，保持一个会话，在这个会话中通过Flink客户端（例如CLI、REST API或Web UI）提交作业。集群启动时所有资源就都已经确定了，所以所有提交的作业会竞争集群中确定的资源。会话模型比较适合于单个规模小、执行时间短的大量作业
    ![img.png](会话模式.png)
    * 单作业模式：会话模式因为资源共享导致很多问题，所以为了更好地隔离资源，就出现了单作业模式。每个作业都有自己的集群。需要注意的是，Flink本身无法直接运行单作业模式，一般需要借助一些资源管理框架来启动集群，比如：YARN、K8S等
    ![img.png](单作业模式.png)
    * 应用模式：前两种模式的应用代码都是在客户端上执行，然后由客户端提交给JobManager的。但是这种方式客户端需要占用大量网络带宽，去下载依赖和把二进制数据发送给JobManager。我们提交作业一般都是同一个客户端，此时会加重客户端所在节点的资源消耗。解决办法就是：不要客户端，直接把应用提交到JobManager上运行。而这也代表着，我们需要为每一个提交的应用单独启动一个JobManager，也就是创建一个集群。这个JobManager只为执行这一个应用而存在，执行结束之后JobManager也就关闭了
    ![img.png](应用模式.png)
33. 应用模式和单作业模式都是提交作业之后才创建集群；单作业模式是通过客户端来提交的，客户端解析出的每一个作业对应一个集群；而应用模型下，是直接由JobManager执行应用程序的
34. <mark>Flink的运行模式：</mark>
    * Standalone模式：Standalone模式是在一个独立的集群中运行Flink。它需要手动启动Flink集群，并且需要手动管理资源。Standalone模式的优点是部署简单，可以跨多台机器运行，缺点是需要手动管理资源，不能直接用`bin/start-cluster.sh`启动集群，需要手动打开JobManager和TaskManager。此模式用的很少
    * <mark>YARN模式：YARN上部署的过程是：客户端把Flink应用提交给YARN的ResourceManager，YARN的ResourceManager会向YARN的NodeManager申请容器。在这些容器上，Flink会部署JobManager和TaskManager的实例，从而启动Flink集群。Flink会根据运行在JobManager上的作业所需要的Slot数量动态分配TaskManager资源，如果没有运行的作业就会回收</mark>（这个模式下，Flink作为YARN的一个应用程序运行在YARN集群中。Flink会从YARN获取所需的资源来运行JobManager和TaskManager。如果你已经有了一个运行Hadoop/YARN的大数据平台，选择这个模式可以方便地利用已有的资源，这是企业中用的比较多的方式）
    ![img.png](启动HDFS和YARN.png)
    ![img.png](YARN集群.png)
    YARN运行模式上以会话模式启动：
    `./yarn-session.sh`    
    ![img.png](YARN会话模式.png)
    提交作业，此时会自动分配TaskManager和Task slot：
    ![img.png](YARN模式作业提交前.png)
    ![img.png](YARN下提交作业.png)
    当然把作业取消，此时YARN也会管理自动释放TaskManager和Task slot：
    ![img.png](YARN管理作业取消后自动释放资源.png)
    YARN运行模式上以单作业模式启动并提交作业：
    `bin/flink run -d -t yarn-per-job -c org.example.WordCountStreamUnboundDemo Flink1.17-1.0-SNAPSHOT.jar`
    ![img.png](YARN单作业.png)
35. <mark>使用YARN模式必须要先启动Hadoop。Flink 的 YARN 模式本质上是把 Hadoop YARN 作为资源调度器，让 YARN 来管理 Flink 任务的资源分配和集群生命周期。要让它正常工作，必须先启动 Hadoop 里的HDFS和YARN服务</mark>
36. <mark>`jps`可以用来查看java进程</mark>
37. `tail -f $HADOOP_HOME/logs/hadoop-root-*-*.log`：查看Hadoop日志
38. 