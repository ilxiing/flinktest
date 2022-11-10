package com.cici.flink05;

import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class SinkToKafka {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    // 1. 从 kafka 中读取数据
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "kafka:9092");
    DataStreamSource<String> kafkaStream = env.addSource(
        new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));
    //./kafka-console-producer.sh --broker-list kafka:9092 --topic clicks
    //./kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic events

    // 2. flink 进行数据转换
    SingleOutputStreamOperator<String> result = kafkaStream.map(new MapFunction<String, String>() {
      @Override
      public String map(String value) throws Exception {
        String[] fields = value.split(",");
        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
      }
    });

    // 3. 将结果数据写入 kafka
    result.addSink(new FlinkKafkaProducer<String>("kafka:9092", "events", new SimpleStringSchema() ));

    env.execute();


  }
}
