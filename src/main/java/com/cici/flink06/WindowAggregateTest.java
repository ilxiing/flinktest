package com.cici.flink06;

import com.cici.flink05.ClicksSource;
import com.cici.flink05.Event;
import java.time.Duration;
import java.util.HashSet;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowAggregateTest {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    // 设置水位线
    DataStreamSource<Event> sourceStream = env.addSource(new ClicksSource());
    //sourceStream.print();
    SingleOutputStreamOperator<Event> stream = sourceStream.assignTimestampsAndWatermarks(
        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
              @Override
              public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
              }
            })
    );
    stream.print();
    stream.keyBy(data -> true)
        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
        .aggregate(new AvgPv()).print();

    env.execute();


  }

  public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>{

    @Override
    public Tuple2<Long, HashSet<String>> createAccumulator() {
      return Tuple2.of(0L, new HashSet<String>());
    }

    @Override
    public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
      accumulator.f1.add(value.user);
      return Tuple2.of(accumulator.f0+1, accumulator.f1);
    }

    @Override
    public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
      return (double) accumulator.f0/accumulator.f1.size();
    }

    @Override
    public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
      return null;
    }
  }

}
