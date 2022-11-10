package com.cici.flink05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformFlatmap {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    DataStreamSource<Event> stream = env.fromElements(
        new Event("roy", "./cart", 1000L),
        new Event("cici", "./home", 2000L)
    );
    // 1. 自定义接口实现
    //SingleOutputStreamOperator<String> reslut = stream.flatMap(new MyFlatmapFunction());
    //reslut.print();

    // 2. 匿名函数
//    SingleOutputStreamOperator<String> stream1 = stream.flatMap(
//        new FlatMapFunction<Event, String>() {
//          @Override
//          public void flatMap(Event value, Collector<String> out) throws Exception {
//            out.collect(value.user);
//            out.collect(value.url);
//            out.collect(value.timestamp.toString());
//          }
//        });
//    stream1.print();

    // 3. lambda 表达式
    SingleOutputStreamOperator<String> stream3 = stream.flatMap((data, out) -> {
      if (data.user.equals("roy")){
        out.collect(data.url);
      } else if (data.user.equals("cici")) {
        out.collect(data.url);
        out.collect(data.timestamp.toString());
      }
    });
    stream3.returns(new TypeHint<String>(){}
    ).print();


    env.execute();
  }

  public static class MyFlatmapFunction implements FlatMapFunction<Event, String> {

    @Override
    public void flatMap(Event value, Collector<String> out) throws Exception {
      out.collect(value.user);
      out.collect(value.url);
      out.collect(value.timestamp.toString());
    }
  }
}
