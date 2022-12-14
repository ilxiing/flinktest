package com.cici.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.concurrent.java8.FuturesConvertersImpl.P;

public class StreamWordCount {

  public static void main(String[] args) throws Exception {
    // 1. 创建流式执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // 从参数中读取 主机名和端口号
    //ParameterTool parameterTool = ParameterTool.fromArgs(args);
    //String hostname = parameterTool.get("host");
    //Integer port = parameterTool.getInt("port");

    // 2. 读取文本流  一般不会写死 hostname 一般配置参数
    DataStreamSource<String> lineDataStream = env.socketTextStream("C15", 7777);
    //DataStreamSource<String> lineDataStream = env.socketTextStream(hostname, port);

    // 3. 转换计算
    SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStream.flatMap(
            (String line, Collector<Tuple2<String, Long>> out) -> {
              String[] words = line.split(" ");
              for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
              }
            })
        .returns(Types.TUPLE(Types.STRING, Types.LONG));

    // 4. 分组
    KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

    // 5. 汇总计算
    SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

    // 6. 打印
    sum.print();

    // 7. 启动执行
    env.execute();
  }
}
