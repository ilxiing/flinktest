package com.cici.flink05;

import java.util.Calendar;
import java.util.Random;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ClicksSource implements SourceFunction<Event> {
  // 声明一个标志位控制数据的生成
  private Boolean running=true;


  @Override
  public void run(SourceContext<Event> ctx) throws Exception {
    // 随机生成数据
    Random random = new Random();
    String [] users = {"Marry", "Alice", "Bob", "Cary"};
    String [] urls = {"./home", "./cart", "./fav", "./prod?id=10"};

    // 循环生成数据
    while(running){
      String user = users[random.nextInt(users.length)];
      String url = urls[random.nextInt(urls.length)];
      Long timestamp = Calendar.getInstance().getTimeInMillis();
      ctx.collect(new Event(user, url, timestamp));

      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    running=false;
  }
}
