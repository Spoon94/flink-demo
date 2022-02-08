package com.alibaba.c2m.module01.wordcount;

import com.google.common.base.Splitter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author qingyong
 * @date 2022/02/08
 */
public class WordCountStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        DataStreamSource<String> textStream = env
            .socketTextStream("127.0.0.1", 9000, "\n");

        DataStream<WordAndCount> wordAndCountDataStream = textStream
            .flatMap(new FlatMapFunction<String, WordAndCount>() {
                @Override
                public void flatMap(String s, Collector<WordAndCount> collector) throws Exception {
                    Splitter.on(" ").splitToStream(s)
                        .map(word -> new WordAndCount(word, 1))
                        .forEach(wordAndCount -> collector.collect(wordAndCount));
                }
            })
            .keyBy("word")
            .timeWindow(Time.seconds(30), Time.seconds(30))
            .reduce((w1, w2) -> {
                return new WordAndCount(w1.getWord(), w1.getFrequency() + w2.getFrequency());
            });
        wordAndCountDataStream.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }

}
