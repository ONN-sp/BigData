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
    * NameNode（主管理节点）：在单个 HDFS Namespace 下，只有一组 NameNode（HA 模式下为 Active+Standby）；但整个集群可以通过 Federation 部署多组 NameNode。用于保存文件系统元数据：文件夹、文件名、文件由哪些 Block 组成、Block 在哪个 DN。不存真实业务数
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
22. 提交作业=提交flink应用
23. <mark>所有的Flink程序都可以归纳为三部分构成：Source数据源会源源不断的产生数据，Transformation将产生的数据进行各种业务逻辑的数据处理，最终由Sink输出到外部（console、kafka、redis、DB...）：</mark>
    * `Source`：”源算子“，负责读取数据源
    * `Transformation`：”转换算子“，利用各种算子进行处理加工（比如：keyBy、sum、map等）
    * `Sink`：”下沉算子“，负责数据的输出
24. <mark>所有基于Flink开发的程序都能够映射成一个Dataflows图</mark>
    ![img.png](Dataflows图.png)
    <mark>当Source数据源的数量比较大或计算逻辑相对比较复杂的情况下，需要提高并行度来处理数据，采用并行数据流。通过设置不同算子的并行度，比如Source并行度设置为2，map也是2，此时会启动2个并行的线程来处理数据</mark>
    ![img.png](并行处理.png)
25. DataSet API只能是批处理，它是一口气把所有数据拿到后再处理，而不是来一条处理一条，以WordCut为例，其输出结果为：
    ![img.png](DataSetAPI的WordCut.png)
    而DataStream API是流处理，它是来一条处理一条，以WordCut为例，其输出结果为：
    ![img.png](DataStreamAPI的WordCut.png)（前面的输出编号指的是：并行度，根据自己电脑线程数决定的）
    这个结果也能体现“有状态计算”，在聚合得到3时，2会被Flink自身保存维护，没有用到外部redis等保存
26. 从Flink1.12开始，官方推荐的做法是直接使用`DataStream API`，对于要进行批处理只需要将执行模式设为`BATCH`即可，如`env.setRuntimeMode(RuntimeExecutionMode.BATCH);`
27. DataStream API中必须调用`env.excute()`
28. <mark>java的泛型会在编译阶段进行泛型擦除，如果要使用泛型类型进行链式调用后续方法，需要显示指定泛型类型，否则会报错，比如：</mark>
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
29. Flink集群是一个典型的Master-Slave架构，架构中包含了两个重要角色，分别是「JobManager」和「TaskManager」。 JobManager相当于是Master，TaskManager相当于是Slave：
    ![img.png](JobManagerTaskManager.png)
    <mark>在Flink中，JobManager负责整个Flink集群任务的调度以及资源的管理。它从客户端中获取提交的应用，然后根据当前Flink集群中TaskManager上TaskSlot的使用情况，为提交的应用分配相应的TaskSlot资源并命令TaskManager启动从客户端中获取的应用。 TaskManager则负责执行作业流的Task，并且缓存和交换数据流。 在TaskManager中资源调度的最小单位是Task slot，默认一个TaskManager有一个Task slot。TaskManager中Task slot的数量表示并发处理Task的数量，slot的数量限制了TaskManager能够并行处理的任务数量。
    一台机器节点可以运行多个TaskManager，TaskManager工作期间会向JobManager发送心跳保持连接</mark>
30. <mark>编写的Flink作业代码可以通过`WEB UI`(这个UI是Flink JobManager 的 Web UI)提交(Apache Flink 集群的 Web UI 监控页面)，也可以直接命令行提交`bin/flink run -m [JobManager地址:port] -c [作业的全类名] [jar包路径]`，命令行提交更常用</mark>
31. 可以在`Apache Flink`上查看当前部署的集群，此时可以`localhost:8081`或者`172.17.87.132:8081`访问Flink集群的WEB UI监控页面：
    ![img.png](WEBUI.png)(这是Apache Flink 的 Web UI 监控面板)
    在wsl中启动nc，然后提交作业到WEB UI，查看其日志：
    ![img.png](WEBUI日志.png)
32. WSL和Windows是不同的操作系统，Flink集群部署到wsl中，那么就要在wsl中打开`nc -l -p 7777`，而不是在windows中打开，虽然此时可以通过windows的WEB UI看到WSL中部署的flink集群，那是因为：Windows 是它的 “宿主机”，两者之间网络是通的，像同一台电脑上的两个系统，
你在 WSL2 里启动一个服务，监听 0.0.0.0:8081（Flink 就是这样，flink-conf.yaml就配置了`rest.bind-address: 0.0.0.0`，表示监听所有网卡、所有 IP）。 WSL2 会自动把这个端口 “映射” 到 Windows 的 localhost 上。 所以：Windows 浏览器访问 localhost:8081，或者访问WSL的IP 172.17.87.132:8081都能直接连到WSL里的Flink JobManager。
但是不能写成127.0.0.1，就只能 WSL 内部访问，Windows 就打不开了
33. Flink为各种场景提供了不同的部署模式：
    * 会话模式：需要提前启动一个集群，保持一个会话，在这个会话中通过Flink客户端（例如CLI、REST API或Web UI）提交作业。集群启动时所有资源就都已经确定了，所以所有提交的作业会竞争集群中确定的资源。会话模型比较适合于单个规模小、执行时间短的大量作业
    ![img.png](会话模式.png)
    * 单作业模式：会话模式因为资源共享导致很多问题，所以为了更好地隔离资源，就出现了单作业模式。每个作业都有自己的集群。需要注意的是，Flink本身无法直接运行单作业模式，一般需要借助一些资源管理框架来启动集群，比如：YARN、K8S等（对于每个提交的作业，都会启动一个新的 Flink 集群，然后再执行该作业。作业完成后，相应的 Flink 集群也会被终止。这种模式适合长时间运行的作业）
    ![img.png](单作业模式.png)
    * 应用模式：前两种模式的应用代码都是在客户端上执行，然后由客户端提交给JobManager的。但是这种方式客户端需要占用大量网络带宽，去下载依赖和把二进制数据发送给JobManager。我们提交作业一般都是同一个客户端，此时会加重客户端所在节点的资源消耗。解决办法就是：不要客户端，直接把应用提交到JobManager上运行。而这也代表着，我们需要为每一个提交的应用单独启动一个JobManager，也就是创建一个集群。这个JobManager只为执行这一个应用而存在，执行结束之后JobManager也就关闭了。一个Flink应用会启动一个Flink集群（这种模式是一种特殊的 Per-Job 模式，它允许用户以反应式的方式与作业进行交互（比如，使用 DataStream API）。这是 Flink 1.11 版本引入的新模式，它结合了Session模式和Per-Job模式的优点。在Application模式下，每个作业都会启动一个独立的Flink集群，但是作业提交快）
    ![img.png](应用模式.png)
34. 应用模式和单作业模式都是提交作业之后才创建集群；单作业模式是通过客户端来提交的，客户端解析出的每一个作业对应一个集群；而应用模型下，是直接由JobManager执行应用程序的
35. 不管是什么部署模式，一个作业对应一个新的jobmaster
36. <mark>应用模式和单作业模式提交一次作业，就启动一个新的集群，即会完整启动一套全新 JobManager；而会话模式不会启动新的集群，只是会一个作业对应一个新的jobmaster</mark>
37. <mark>Flink的运行模式：</mark>
    * Standalone模式：Standalone模式是在一个独立的集群中运行Flink。它需要手动启动Flink集群，并且需要手动管理资源，`bin/start-cluster.sh`启动的就是Flink独立集群模式-Standalone模式中的会话模式。Standalone模式的优点是部署简单，可以跨多台机器运行，缺点是需要手动管理资源。对于Standalone模式中的应用模式不能直接用`bin/start-cluster.sh`启动集群，需要使用脚本手动打开JobManager和TaskManager。此模式用的很少
    * <mark>YARN模式：YARN上部署的过程是：客户端把Flink应用提交给YARN的ResourceManager，YARN的ResourceManager会向YARN的NodeManager申请容器。在这些容器上，Flink会部署JobManager和TaskManager的实例，从而启动Flink集群。Flink会根据运行在JobManager上的作业所需要的Slot数量动态分配TaskManager资源，如果没有运行的作业就会回收</mark>（这个模式下，Flink作为YARN的一个应用程序运行在YARN集群中。Flink会从YARN获取所需的资源来运行JobManager和TaskManager。如果你已经有了一个运行Hadoop/YARN的大数据平台，选择这个模式可以方便地利用已有的资源，这是企业中用的比较多的方式）
    启动YARN模式：`start-dfs.sh`、`start-yarn.sh`
    ![img.png](启动HDFS和YARN.png)
    ![img.png](YARN集群.png)(这是Hadoop YARN ResourceManager 的 Web UI 监控页面)
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
    YARN运行模式上以应用模式启动并提交作业：
    `bin/flink run-application -t yarn-application -c org.example.WordCountStreamUnboundDemo ./Flink1.17-1.0-SNAPSHOT.jar`
    ![img.png](YARN运行模式应用模式.png)
38. <mark>Standalone模式下，`bin/start-cluster.sh`启动的就是Flink独立集群模式-Standalone模式中的会话模式</mark>
39. <mark>使用YARN模式必须要先启动Hadoop。Flink 的 YARN 模式本质上是把 Hadoop YARN 作为资源调度器，让 YARN 来管理 Flink 任务的资源分配和集群生命周期。要让它正常工作，必须先启动 Hadoop 里的HDFS和YARN服务</mark>
40. <mark>`jps`可以用来查看java进程</mark>
41. `tail -f $HADOOP_HOME/logs/hadoop-root-*-*.log`：查看Hadoop日志
42. <mark>为了更快速上传application，可以先把：flink依赖、jar包这些提前上传到HDFS中，然后jobmanager直接从HDFS中读取这些依赖，就不用每一次都本地上传了。此时任务启动时不再重复上传 Flink 基础依赖，仅加载业务 Jar，启动速度大幅提升，这种方式在多任务、频繁提交作业的场景收益明显</mark>
    * 上传flink依赖：`hadoop fs -mkdir /flink-dist`、`hadoop fs -put lib/ /flink-dist`、`hadoop fs -put plugins/ /flink-dist`
    * 上传jar包：`hadoop fs -mkdir /flink-jars`、`hadoop fs -put Flink1.17-1.0-SNAPSHOT.jar /flink-jars`
    * 上传到hdfs后可以在网页查看`http://localhost:9870/explorer.html#/`
    ![img.png](BrowingHDFS.png)(这是Hadoop HDFS NameNode 的 Web UI 监控页面)
    * 通过上传到hdfs的依赖和jar包，可以直接利用它们提交作业：` bin/flink run-application -t yarn-application -Dyarn.provided.lib.dirs="hdfs://localhost:9000/flink-dist" -c org.example.WordCountStreamUnboundDemo hdfs://localhost:9000/flink-jars/Flink1.17-1.0-SNAPSHOT.jar`
43. 在实际开发中，推荐使用YARN+应用模式+提前上传依赖和jar包到HDFS的方法。也就是实际开发中：我们一般不会直接
44. 历史服务器：运行Flink的集群一旦停止，只能去yarn或本地磁盘上查看日志，不可以再查看作业挂掉之前的运行的WEB UI，很难清楚知道作业在挂的那一刻到底发生了什么。Flink提供了历史服务器，用来在相应的Flink集群关闭后查询已完成作业的统计信息
    ![img.png](历史服务器.png)
    ![img.png](历史服务器结果.png)
45. Flink运行时架构，以Session模式为例：
    ![img.png](会话模式运行架构.png)（每个提交的作业，都会在 JobManager 里启动一个独立的 JobMaster 实例，从头到尾管这一个 Job。一个作业对应一个jobmaster）
46. <mark>算子：对流式数据做处理、转换、计算、输出的逻辑单元，是 Flink 程序里最基本的运算步骤。Flink算子分类：Source 数据源算子(`socketTextStream(host, port)`、`readTextFile()`)、Transformation 转换算子(`map`、`filter`、`keyBy`)、Sink 输出算子(`print()`)</mark> 
47. 在Flink执行过程中，每一个算子（operator）可以包含一个或多个子任务，这些子任务在不同的线程、不同的物理机或不同的容器中完全独立地执行。一个特定算子的子任务的个数被称之为其并行度。整个流处理程序的并行度，就应该是所有算子并行度中最大的那个，这代表了运行程序需要的 slot 数量
48. Flink中每一个算子的并行度可以不一样，设置方法是：`setParallelism(并行度)`，比如给`socketTextStream`算子设置并行度1：` DataStreamSource<String> socketDS = env.socketTextStream("localhost", 7777).setParallelism(1);`
49. Flink的流式代码：数据源 (Source) → 一系列算子 → 输出 (Sink)
50. <mark>`StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());`：创建带内置 Web UI 的本地 Flink 流运行环境（没有独立的外部集群，是在当前 Java 进程内模拟的嵌入式本地迷你集群，跑在你运行代码的这台机器），本地运行任务时，可通过网页直观监控作业、算子、Slot、数据流等状态。此时在本地开发环境的IDEA中运行代码后，默认访问：http://localhost:8081，和独立 Flink 集群 Web UI 界面完全一样。这种方法仅用于本地开发、测试、debug；生产环境绝对不用</mark>
    ![img.png](本地Flink集群.png)
51. 全局设置并行度，所有算子都一样`env.setParallelism(3)`，还可以在WEB UI或命令行(可以增加 -p 参数来指定当前应用程序执行的并行度)提交作业时指定。并行度优先级：算子设置并行度>代码全局设置并行度>命令行或WEB UI提交时指定>flink-conf.yaml配置文件
52. Flink中算子之间的传输关系：one to one；重分区
53. <mark>算子链（这是一种优化措施）：将算子链接成task是非常有效的优化，可以减少线程之间的切换和基于缓存区的数据交换，在减少时延的同时提升吞吐量（在 Flink 中，Task 是一个阶段多个功能相同 subTask 的集合，Flink 会尽可能地将 operator 的 subtask 链接（chain）在一起形成 task。每个 task 在一个线程中执行。将 operators 链接成 task 是非常有效的优化：它能减少线程之间的切换，减少消息的序列化/反序列化，减少数据在缓冲区的交换，减少了延迟的同时提高整体的吞吐量。）</mark>
    * Flink中算子串子一起的条件
      * one to one
      * 并行度相同
    * <mark>Flink中，并行度相同的one to one算子操作，可以直接链接在一起形参一个”大“的任务，此时这两个算子操作是forward关系。这样原来的算子就称为了真正任务里的一部分。每个task会被一个线程执行，这样的技术就是算子链。比如下图：原来本来Source、map是4个子任务、2个任务，现在合并后变成了2个子任务，1个任务了</mark>
    ![img.png](算子链.png)
    * <mark>和Spark类似，其实Flink中task的划分也是依据宽依赖（只是在flink中叫重分区），当出现宽依赖就会一个新的task</mark>
    ![img.png](task划分.png)
    * 禁用算子链：
      * 全局禁用算子链：`env.disableOperatorChaining()`
      * 某个算子不参与链化：`A.disableChaining()`；算子A不会与前面和后面的算子的串在一起
      * 从某个算子开启新链条：`A.startNewChain()`；算子A不与前面串在一起，从A开始正常链化
54. WEB UI中一个框框就是一个任务，从这个框框中也可以看出分task的依据：因为HASH(对应的就是`.keyBy()`)是一个重分区操作（也就是遇到了宽依赖），那么就是一个新的task
    ![img.png](WEBUItask.png)
55. Flink中每一个TaskManager都是一个JVM进程，它可以启动多个独立的线程，来并行执行多个子任务。为了控制并发量，需要在TaskManager上对每个任务运行所占用的资源做出明确划分，这就是所谓的task slots
56. 每个task slot表示的是TaskManager拥有计算资源的一个固定大小的子集，这些资源就是用来独立执行一个子任务的
57. <mark>同一TaskManager 内不同 Task Slot 相互独立、并发执行</mark>
58. <mark>Task slot和子任务的关系：</mark>
    * 常规情况：一个 SubTask 占用 一个 Task Slot
    * 算子链场景：多个上下游算子被链在一起，合并成一个 Task，对应一个 SubTask，依旧只占用 一个 Slot
    * 共享 Slot 组：Flink 默认开启槽位共享：同一个作业的不同算子的子任务，可以共用同一个 Slot
59. `flink-conf.yaml`配置文件中可以设置每个TaskManager的Slot数量：`taskmanager.numberOfTaskSlots: 10`，可以给每个TM配置不同的slot数量
60. <mark>需要注意的是：task slot对内存是硬隔离的，而cpu不是硬隔离。slot目前仅仅用来隔离内存，不会涉及cpu的隔离，cpu还是大家共用的。在具体开发时，可以将slot数量配置为机器的cpu核心，尽量避免不同任务之间对cpu的竞争。因为：Slot 只是线程分组，不会用操作系统机制把某个 Slot 绑定到指定 CPU 核心。所有 Slot 的线程共用机器 CPU 核心，运行时会互相抢占 CPU 时间片，做不到彻底隔离</mark>
61. <mark>不同的Task下的subtask要分发到同一个TaskSlot中，降低数据传输、提高执行效率；相同的Task下的subtask要分发到不同的TaskSlot中，以提高并行度</mark>
    ![img.png](分发规则.png)
62. 资源充足情况下：相同算子的不同子任务，分布在不同 Slot 并行运行；资源不足时：多个同算子的子任务会运行在同一个 Slot，串行执行
63. task slot特点：
    * 均分隔离内存，不隔离cpu
    * 可以共享：同一个job中，不同算子的子任务才可以共享同一个slot，此时同时在运行的，前提是，属于同一个slot共享租，默认都是"default"
64. <mark>flink默认是允许slot共享的，如果希望某个算子对应的任务完全独占一个 slot，或者只有某一部分算子共享 slot，在Flink中，可以通过在代码中使用`slotSharingGroup()`方法来设置slot共享组。Flink会将具有相同slot共享组的操作放入同一个slot中，同时保持不具有slot共享组的操作在其他slot中。这可以用来隔离slot：</mark>
    `dataStream.map(...).slotSharingGroup("group1");`：这样，只有属于同一个 slot 共享组的子任务，才会开启 slot 共享，不同组之间的任务是完全隔离的，必须分配到不同的 slot 上
65. 默认的slot共享组是"default"，如果未指定slot共享组，那么所有子任务都会被分配到"default"组中，此时所有算子操作都是一个slot共享组
66. <mark>流程序中最大算子并行度=运行所需要的slot数量</mark>
67. 并行度和slots数量的关系；
    * slots是一种静态的概念，表示最大的处理并发上限
    * 并行度是一种动态的概念，表示实际运行占用了几个
    * <mark>要求：slot数量>=job并行度（算子最大并行度），job才能运行，不然运行失败(这是standalone模式)。如果是YARN模式，它会自动根据提交的job的并行度，来申请taskManager的数量（申请规则：taskManager的数量=job并行度/slot数量，向上取整）</mark>
    ![img.png](并行度和slots数量.png)
68. Standalone会话模式作业提交流程：
    ![img.png](Standalone会话模式提交流程.png)（逻辑流图到作业流图最重要的就是进行算子链优化；jobmaster把作业流图转换为执行图）
69. YARN应用模式作业提交流程：
    ![img.png](YARN应用模式提交作业流程.png)
70. DataStream API代码由以下几部分组成：
    ![img.png](Flink代码.png)
71. DataStream API：
    * 创建执行环境：`StreamExecutionEnvironment.getExecutionEnvironment()`：自动识别是远程集群还是本地IDEA环境，可以通过`new Configuration()`来指定：
      * `Configuration conf = new Configuration();     conf.set(RestOptions.BIND_PORT, "8082");      StreamExecutionEnvironment.getExecutionEnvironment(conf)`：这是设置的IDEA打开的本地Flink集群端口，与Standalone、YARN模式等无关
    * Flink流批一体，代码api是一套，默认是流处理，设置为批处理：`env.setRuntimeMode(RuntimeExecutionMode.BATCH);`；批处理设置也可以在命令行提交作业时指定：`-Dexecution.runtime-mode=BATCH`
    * 最后一定要有一个程序的触发执行，前面其实只是定义了作业的每个执行操作，然后添加到数据流图中，这时并没有真正处理数据——因为数据可能还没来，只有等到数据到来，才会从触发真正的计算，这也被称为“延迟执行”：`env.execute();`
    * 默认`env.execute`触发一个flink job，一个main方法可以调用多个execute，但是没意义，指定到第一个就会阻塞住；`env.executeAsync();`表示异步触发，不阻塞，此时一个main方法里面`env.executeAsync();`的个数=生成的flink job(一个flink job对应一个jobmaster)数
