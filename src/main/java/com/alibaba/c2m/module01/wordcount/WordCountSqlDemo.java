package com.alibaba.c2m.module01.wordcount;

import com.google.common.base.Splitter;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @author qingyong
 * @date 2022/02/08
 */
public class WordCountSqlDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbEnv);

        String words = "hello flink hello spark";
        List<WordAndCount> wordAndCountList = Splitter.on(" ").splitToStream(words).map(word->new WordAndCount(word,1)).collect(
            Collectors.toList());

        DataSet<WordAndCount> wordAndCountDataSet = fbEnv.fromCollection(wordAndCountList);

        Table rawTable = fbTableEnv.fromDataSet(wordAndCountDataSet);
        rawTable.printSchema();
        fbTableEnv.createTemporaryView("WordCount",rawTable);
        Table cntTable = fbTableEnv
            .sqlQuery("select word as word, sum(frequency) as frequency from WordCount GROUP BY word");

        DataSet<WordAndCount> wordAndCountResDataSet = fbTableEnv
            .toDataSet(cntTable, WordAndCount.class);
        wordAndCountResDataSet.print();

    }

}
