package org.example.Source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从文件读取数据
        FileSource<String> filesource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("E:/BigData/MavenFlink1.17/input/word.txt")
                )
                .build();
        DataStreamSource<String> res = env.fromSource(filesource, WatermarkStrategy.noWatermarks(), "filesource");
        res.print();
        env.execute();
    }
}
