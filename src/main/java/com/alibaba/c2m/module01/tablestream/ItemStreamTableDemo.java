package com.alibaba.c2m.module01.tablestream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author qingyong
 * @date 2022/02/08
 */
public class ItemStreamTableDemo {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode()
            .useBlinkPlanner().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment
            .getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment
            .create(streamEnv, envSettings);
        DataStreamSource<Item> rawItemStreamSource = streamEnv.addSource(new ItemSourceImpl());
        OutputTag<Item> evenOutPutTag = new OutputTag<Item>("even", TypeInformation.of(Item.class));

        SingleOutputStreamOperator<Item> oddItemStream = rawItemStreamSource
            .process(new ProcessFunction<Item, Item>() {
                @Override
                public void processElement(Item value, Context ctx, Collector<Item> out)
                    throws Exception {
                    if (value.getId() % 2 == 0) {
                        ctx.output(evenOutPutTag, value);
                    } else {
                        out.collect(value);
                    }
                }
            });
        DataStream<Item> evenItemStream = oddItemStream.getSideOutput(evenOutPutTag);

        streamTableEnv.createTemporaryView("even_table", evenItemStream, "name,id");
        streamTableEnv.createTemporaryView("odd_table", oddItemStream, "name,id");

        Table queryTable = streamTableEnv.sqlQuery(
            "select odd.id,odd.name,even.id,even.name from even_table as even join odd_table as odd on even.name=odd.name");

        queryTable.printSchema();
        streamTableEnv.toRetractStream(queryTable,
            TypeInformation.of(new TypeHint<Tuple4<Integer, String, Integer, String>>() {
            })).print();

        streamEnv.execute("Item Stream Table");
    }

}
