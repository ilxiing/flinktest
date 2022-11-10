package com.cici.flink06;

import com.cici.flink05.ClicksSource;
import com.cici.flink05.Event;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WaterMarkTest {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    env.addSource(new ClicksSource())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                  @Override
                  public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp;
                  }
                })
        ).print();

    env.execute();
  }

}
