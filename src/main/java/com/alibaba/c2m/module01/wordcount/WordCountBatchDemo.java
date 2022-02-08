package com.alibaba.c2m.module01.wordcount;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author qingyong
 * @date 2022/02/08
 */
public class WordCountBatchDemo {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> wordList = Lists.newArrayList("Flink Spark Storm",
            "Flink Flink Flink",
            "Spark Spark Spark",
            "Storm Storm Storm");
        DataSet<String> wordListSource = env.fromCollection(wordList);
        DataSet<Tuple2<String, Integer>> counts = wordListSource.flatMap(
            new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String str, Collector<Tuple2<String, Integer>> collector)
                    throws Exception {
                    Splitter.on(" ").splitToStream(str)
                        .map(String::toLowerCase)
                        .map(word -> new Tuple2(word, 1))
                        .forEach(tuple2 -> collector.collect(tuple2));
                }
            }).groupBy(0)
            .sum(1);
        counts.print();
//        counts.printToErr();
//        env.execute("Word Count ");
    }

}
