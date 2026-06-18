1. kafka中的基础概念：
    ```txt
    1.Topic(话题)：Kafka中用于区分不同类别信息的类别名称。由producer指定
    2.Producer(生产者)：将消息发布到Kafka特定的Topic的对象(过程)
    3.Consumers(消费者)：订阅并处理特定的Topic中的消息的对象(过程)
    4.Broker(Kafka服务集群)：已发布的消息保存在一组服务器中，称之为Kafka集群。集群中的每一个服务器都是一个代理(Broker). 消费者可以订阅一个或多个话题，并从Broker拉数据，从而消费这些已发布的消息。
    5.Partition(分区)：Topic物理上的分组，一个topic可以分为多个partition，每个partition是一个有序的队列。partition中的每条消息都会被分配一个有序的id（offset）
    6.Replication:每一个分区都有多个副本，副本的作用是做备胎。当主分区（Leader）故障的 时候会选择一个备胎（Follower）上位，成为Leader
    7.Consumer Group
    ```
2. kafka实例（一个实例通常就是一个broker，多个broker组成集群）作为中间的消息队列，让上下游服务完全解耦，并且对于下游消费者可以根据不同的消费者组进行负载均衡。对于生产者发送的消息是有类别的，每一个消息类对应一个topic，而每一个topic对应多个分区，每个分区是有序的，分区可以在不同的broker上，即实现分布式存储。每一个分区可以有多个副本，也就是follwer，实现高可用性
3. kfaka原理:
   * Producer：Producer即生产者，消息的产生者，是消息的⼊口。
   * kafka cluster：kafka集群，一台或多台服务器组成
     - Broker：Broker是指部署了Kafka实例的服务器节点。每个服务器上有一个或多个kafka的实例，我们姑且认为每个broker对应一台服务器。每个kafka集群内的broker都有一个不重复的 编号，如图中的broker-0、broker-1等……
     - Topic：消息的主题，可以理解为消息的分类，kafka的数据就保存在topic。在每个broker上 都可以创建多个topic。实际应用中通常是一个业务线建一个topic。
     - Partition：Topic的分区，每个topic可以有多个分区，分区的作用是做负载，提高kafka的吞 吐量。同一个topic在不同的分区的数据是不重复的，partition的表现形式就是一个一个的⽂件夹！
     - Replication:每一个分区都有多个副本，副本的作用是做备胎。当主分区（Leader）故障的 时候会选择一个备胎（Follower）上位，成为Leader。在kafka中默认副本的最大数量是10 个，且副本的数量不能大于Broker的数量，follower和leader绝对是在不同的机器，同一机 器对同一个分区也只可能存放一个副本（包括自己）。
   * Consumer：消费者，即消息的消费方，是消息的出口。
   * Consumer Group：我们可以将多个消费组组成一个消费者组，在kafka的设计中同一个分 区的数据只能被消费者组中的某一个消费者消费。同一个消费者组的消费者可以消费同一个 topic的不同分区的数据，这也是为了提高kafka的吞吐量
4. kafka保证高性能、高吞吐的关键
   * 顺序读写，所有消息基本都是顺序追加而不是随机io
   * 页缓存：当写入消息时，数据并不是直接刷到磁盘上，而是先写入页缓存（内存一部分），因此写入速度极快
   * 批量处理：生产者会将多条消息聚合成一个批次再发送给broker，大大减少网络请求次数，提高吞吐量
   * 分区机制：一个topic的多个partition是可以并发读写
   * leader/follwer保证的是高可用
5. kafka保证消息能消费成功的机制：
   * 发送端：
    1. ack机制（没有确认会重发），利用acks参数控制。ack=0是不等任何确认，发完就完；ack=1只等leader确认；ack=acll需要等learder和follwer都确认才行；
    2. 重试机制：设置retries>0失败时生产者会自动重试发送失败的消息，配合enable.idempotence=true开启幂等性，避免重复写入；
   * 消费端
    1. offset提交机制：通过offset记录消费进度，消费者可以手动或自动提交offset给kafka（提交的 Offset 会被存储在 Kafka 一个名为 __consumer_offsets 的内部 Topic 中），这样做的目的是消费者可以重新消费，但是这个机制有潜在问题：重复消费（消息消费成功和offset提交不是在一个原子操作中，如果offset未提交但是消息已经消费了，就可能导致重复消费）；消息丢失（如果消息消费失败但是offset提交了，就会导致这条消息丢失）
6. acks机制（broker端不丢消息）：这是生产者端的配置，用于控制消息发送的可靠性级别
   * acks=0:不等待任何确认，生产者发送完消息后立即认为成功，不等待服务器响应（可能丢失消息）
   * acks=1:等待leader确认。只要leader成功写入消息，生产者就收到成功响应（如果leader在同步副本前崩溃，消息可能丢失，因为leader确认后就不会重发了）
   * acks=all或acks=-1:等待leader/follwer均确认，才返回成功（可靠性最高，但是延迟会增加，吞吐下降）
7. 消费者组的不同消费者是可以并行的
8. 一般消费就是：一个消费者组里的消费者各自独占若干个分区去串行消费消息，单个消费者消费分区是串行的，但是不同消费者消费不同分区时是并行的
9. <mark>kafka的Sequence Number（序列号）：Sequence Number 是生产给每一条消息分配的单调递增本地序列号，由生产者自己维护，不是 broker 生成，broker生成的是partition的offset。每个生产者客户端（KafkaProducer）针对同一个主题分区维护一个单调递增的 sequence 序列号，用来实现幂等生产者 Idempotent Producer，解决重复发送消息问题</mark>
10. <mark>Kafka 幂等生产者完全由生产者端开启，Broker 只做配合校验。生产者幂等性只能解决：同一生产者、同一分区、重试发同一条消息 → 去重</mark>
11. <mark>一条消息要实现幂等去重，靠 3 个标识联合判断：</mark>
     * PID（Producer ID）：TC 分配给 transactional.id 的生产者/broker分配PID（非事务）
     * Partition（目标分区）
     * Sequence Number（当前分区下的自增序号）
     此时：同一个 PID + 同一个分区，如果收到相同 seq → broker 判定为重复消息，直接丢弃，不写入日志、不报错
12. <mark>kafka事务：旨在解决分布式系统中消息一致性的问题，核心目标是：</mark>
    * 原子性：一组消息要么全部成功发送（和消费），要么全部失败，不会出现部分成功的情况
    * 一致性：确保生产者发送的消息和消费者处理的结果在分布式环境下保持一致，特别是在“生产-消费”工作流中
    * 隔离性：事务中的操作对其他生产者或消费者不可见，直到事务提交
13. <mark>事务机制的应用场景：</mark>
    * 生产者事务：一个生产者需要将多条消息发送到多个 Topic/分区，确保这些消息要么全部成功，要么全部失败。例如，订单系统同时发送“订单创建”和“库存扣减”消息
    * <mark>生产者-消费者事务：消费者从一个 Topic 读取消息，处理后将结果写入另一个 Topic，确保“读取-处理-写入”是一个原子操作。例如，流处理系统从输入 Topic 读取数据，处理后写入输出 Topic；
    这是一种典型的流处理链路（消费 - 处理 - 产出）：消费Topic1消息 → 计算转换 → 写入Topic2 → 提交Topic1的offset，此时有两个致命问题：</mark>
      * 下游写入成功，offset 提交失败：
        Topic2 已经写入数据，程序崩溃，offset 没存。重启后重新拉取同一批消息，再次写入 Topic2，数据重复
      * offset 提交成功，下游写入失败：
        offset 标记已消费，程序宕机，Topic2 没有数据，永久丢失消息
      * 出现这两个问题的原因是：offset 存储在 Kafka 内部`__consumer_offsets`topic呢，下游数据写入业务主题，是两个独立写入操作。 幂等生产者只能保证单次发送不重复，但无法让两个不同主题的写入操作原子化
      * 因此事务就解决了这个问题
    * Exactly-Once 语义：Kafka 的事务机制与幂等性结合，可以实现“精确一次”投递（Exactly-Once Semantics，EOS），避免消息丢失或重复
14. <mark>kafka的完整Exactly-Once依赖两大核心组件：幂等性生产者+事务，缺一不可：</mark>
    * 生产精确一次：同一生产者重试发送同一条消息，Broker 只持久化一条，不产生重复日志，依赖的是生产者幂等性
    * 消费 + 生产精确一次（流处理场景，Flink/Spark Streaming）：消费旧消息、处理、产出新消息三者要么全部成功，要么全部回滚，依赖事务
    * 纯消费精确一次：业务执行成功后再提交 offset，失败则不提交，下次重拉，配合业务幂等实现最终只处理一次
15. <mark>kafka事务的实现原理：</mark>
    * 关键组件：
      * 事务生产者（Transactional Producer）： 
        * 事务生产者通过设置 `transactional.id` 启用事务模式。每个事务生产者有一个唯一的标识，用于跟踪事务状态。
        * 事务生产者可以发起、提交或回滚事务
      * 事务协调器（transaction Coordinator）
        * 每个 Kafka Broker 都有一个事务协调器，负责管理事务状态
        * 事务协调器维护一个特殊的 Topic（`__transaction_state`），用于存储事务的元数据（如事务 ID、状态、涉及的分区）
      * 事务日志（Transaction Log）
        * 事务状态存储在 `__transaction_state` Topic 中，具有高可用性和持久性
        * 记录事务的开始、提交或回滚状态
      * 消费者组（Consumer Group）：在“生产者-消费者”事务中，消费者组通过偏移量管理确保消息只被处理一次
      * 控制消息（Control Messages）
        * Kafka 使用特殊的控制消息（如 COMMIT 或 ABORT）标记事务的提交或回滚
        * 消费者在读取消息时会忽略未提交事务的消息
    * 工作流程：
      * 生产者事务工作流程：
        * 初始化事务
          * 生产者通过 initTransactions() 初始化事务，向事务协调器注册 transactional.id
          * 事务协调器为该生产者分配一个Producer ID，并在 __transaction_state Topic 中记录事务状态
        * 开始事务
          * 生产者调用 beginTransaction()，标记事务的开始
          * 此时，生产者进入事务模式，发送的消息会被标记为“事务性消息”
        * 发送消息
          * 生产者将消息发送到目标 Topic/分区
          * 这些消息被写入分区日志，但标记为“未提交”（Pending），对消费者不可见
        * 提交或回滚事务
          * 如果所有消息发送成功，生产者调用 commitTransaction()，事务协调器更新 __transaction_state，将事务标记为“已提交”，并向相关分区写入`COMMIT` 这个控制消息
          * 如果发生错误（如网络中断），生产者调用 abortTransaction()，事务协调器将事务标记为“已回滚”，写入`ABORT`这个控制消息，未提交的消息对消费者不可
        * 消费者读取：消费者配置 isolation.level=read_committed（默认值），只会读取已提交事务的消息，忽略未提交或回滚的消息
      * 生产者-消费者事务（消费上游 Topic → 处理数据 → 发送下游 Topic）的工作流程：在“读取-处理-写入”场景中，事务机制还涉及消费者读取消息和提交偏移量，流程为：
        * 消费者从输入 Topic 读取消息，记录 offsets
        * 消费者处理消息，生成结果
        * 生产者以事务方式将结果写入输出 Topic
        * 生产者通过 sendOffsetsToTransaction() 将消费者的偏移量与事务绑定
        * 提交事务时，偏移量和输出消息同时生效，确保“读取-处理-写入”是一个原子操作
    * 如何保证一致性
      * 原子性：事务中的所有操作（消息写入、偏移量提交）要么全部成功，要么全部失败；事务协调器通过 __transaction_state Topic 跟踪状态，确保事务的完整
      * 隔离性：未提交的事务消息对消费者不可见（通过 isolation.level=read_committed）；控制消息（COMMIT 或 ABORT）明确标记事务边
      * 幂等性：事务生产者结合 enable.idempotence=true 和 transactional.id，确保消息不会重复写入，即使生产者重试；每个消息都有一个唯一的 Producer ID 和 Sequence Number，Broker 会检查重复消息
      * Exactly-Once 语义：事务机制与消费者组的偏移量管理结合，确保消息从生产到消费只处理一次；消费者通过 sendOffsetsToTransaction() 将偏移量与事务绑定，避免重复消费
16. kafka事务的局限：
    * 事务机制涉及额外的协调和日志写入（__transaction_state），会增加延迟和资源消耗
    * 建议在需要强一致性的场景中使用，避免滥用