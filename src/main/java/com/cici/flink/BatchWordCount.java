package com.cici.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
// flink1.12 后 dataset api 软弃用状态
public class BatchWordCount {

  public static void main(String[] args) throws Exception {
    // 1.创建执行环境
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // 2.读取数据源
    DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

    // 3. 将每行数据进行分词,转换成二元组
    FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple =
        lineDataSource
            .flatMap(
                (String line, Collector<Tuple2<String, Long>> out) -> {
                  String[] words = line.split(" ");
                  // 将每个单词转换为二元组输出
                  for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                  }
                })
            .returns(Types.TUPLE(Types.STRING, Types.LONG));

    // 4. 按照 word 进行分组
    //AggregateOperator <Tuple2<String, Long>> sum =
    //wordAndOneTuple.groupBy(0).aggregate(Aggregations.SUM, 1);
    UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
    AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

    // 5. 结果打印输出
    sum.print();
  }
}
