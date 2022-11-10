package com.cici.flink05;

import java.util.ArrayList;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class SourceTest {

  public static void main(String[] args) throws Exception {
    // 创建执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // 方便查看数据顺序

    // 1. 从文件中读取数据
    DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

    // 2. 从集合中读取数据
    ArrayList<Integer> nums = new ArrayList<>();
    nums.add(2);
    nums.add(5);
    DataStreamSource<Integer> numstream = env.fromCollection(nums);

    ArrayList<Event> events = new ArrayList<>();
    events.add(new Event("cici", "./prod", 1000L));
    events.add(new Event("roy", "./cart", 2000L));
    DataStreamSource<Event> stream2 = env.fromCollection(events);

    // 3. 从元素中读取数据
    DataStreamSource<Event> stream3 = env.fromElements(
        new Event("cc", "./home", 3000L),
        new Event("rr", "./home", 4000L)
    );

    // 以上三种是批数据测试用

    // 4. 从 socket 中读取数据 (主要用于流式数据测试)
//    DataStreamSource<String> stream4 = env.socketTextStream("c15", 7777);
//
    stream1.print("1");
    numstream.print("nums");
    stream2.print("2");
    stream3.print("3");
//    stream4.print("4");

    // 5. 从 kafka 中获取数据
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "kafka:9092");
//    properties.setProperty("group.id", "consumer-group");
    properties.setProperty("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("auto.offset.reset", "latest");

    DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>(
            "test",
            new SimpleStringSchema(),
            properties
        ));
    kafkaStream.print("kafka");

    env.execute();

  }
}
