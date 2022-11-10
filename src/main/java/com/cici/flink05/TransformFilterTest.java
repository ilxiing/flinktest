package com.cici.flink05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {

  public static void main(String[] args) throws Exception{
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<Event> stream = env.fromElements(
        new Event("cc", "./home", 3000L),
        new Event("rr", "./home", 4000L)
    );

    // 1. 自定义接口
    //SingleOutputStreamOperator result1 = stream.filter(new MyFilterFunction());
    // 2. 匿名函数
    SingleOutputStreamOperator<Event> result2 = stream.filter(new FilterFunction<Event>() {
      @Override
      public boolean filter(Event value) throws Exception {
        return value.user.equals("cc");
      }
    });
    // 3. lambda 表达式
    //SingleOutputStreamOperator<Event> result = stream.filter(data -> data.user.equals("rr"));
    //result.print();
    //result1.print();
    result2.print();
    env.execute();


  }

  public static class MyFilterFunction implements FilterFunction<Event> {

    @Override
    public boolean filter(Event value) throws Exception {
      return value.user.equals("rr");
    }
  }
}
