package com.cici.flink05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<Event> stream = env.fromElements(new Event("roy", "./home", 1000L),
        new Event("cici", "./prod?id=100", 2000L),
        new Event("roy", "./home", 3000L),
        new Event("roy", ".//prod?id=10", 4000L),
        new Event("roy", "./prod?id=20", 3800L),
        new Event("roy", "./prod?id=30", 6000L)
    );

    // 提取当前用户最后一次访问数据
    stream.keyBy(data -> data.user).max("timestamp").print("max");
    stream.keyBy(data -> data.user).maxBy("timestamp").print("maxby");

    env.execute();

  }
}
