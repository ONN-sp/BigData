package org.example.partition;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {// numPartitions是下游算子的并行度
        return Integer.parseInt(key)%numPartitions;
    }
}

