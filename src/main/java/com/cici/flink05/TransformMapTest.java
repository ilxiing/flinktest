package com.cici.flink05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformMapTest {

  public static void main(String[] args) throws Exception{
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<Event> stream = env.fromElements(
        new Event("cc", "./home", 3000L),
        new Event("rr", "./home", 4000L)
    );

    // 1. 使用自定义类实现 mapFunction 接口
    SingleOutputStreamOperator<String> result1 = stream.map(new myMapFuction());

    // 2. 使用匿名类实现 mapFunction 接口
    SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
      @Override
      public String map(Event value) throws Exception {
        return value.user;
      }
    });

    // 3. 用匿名函数 传入 lambda 表达式   filter
    SingleOutputStreamOperator<String> result = stream.map(data -> data.user).filter(user-> user.contains("c"));

    result.print();

    env.execute();

  }

  public static class  myMapFuction implements MapFunction<Event, String>{

    @Override
    public String map(Event value) throws Exception {
      return value.user;
    }
  }
}
