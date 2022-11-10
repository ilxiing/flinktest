package com.cici.flink05;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFormReduceTest {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<Event> stream = env.fromElements(new Event("roy", "./home", 1000L),
        new Event("cici", "./prod?id=100", 2000L),
        new Event("cici", "./prod?id=100", 3100L),
        new Event("roy", "./home", 3000L),
        new Event("cici", "./prod?id=700", 3000L),
        new Event("roy", ".//prod?id=10", 4000L),
        new Event("roy", "./prod?id=20", 3800L),
        new Event("cici", "./prod?id=200", 4000L),
        new Event("roy", "./prod?id=30", 6000L)
    );

    // 1. 统计每个用户的访问频次
    SingleOutputStreamOperator<Tuple2<String, Long>> clicksByusers = stream.map(data -> Tuple2.of(data.user, 1L)).returns(
            new TypeHint<Tuple2<String, Long>>() {
            }
        ).keyBy(data -> data.f0)
        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
          @Override
          public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2)
              throws Exception {
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
          }
        });

    // 2. 选取当前最活跃的用户
    SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByusers.keyBy(data -> "key")
        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
          @Override
          public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2)
              throws Exception {
            return value1.f1 > value2.f1 ? value1 : value2;
          }
        });

    result.print();
    env.execute();
  }
}
