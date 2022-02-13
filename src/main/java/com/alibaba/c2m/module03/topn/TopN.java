package com.alibaba.c2m.module03.topn;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author: spoon
 * @email: shaozejing@xiaomi.com
 * @create: 2022/2/12-4:28 PM
 */
public class TopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 1000L);

        DataStreamSource<String> source = env
            .socketTextStream("127.0.0.1", 9000, "\n");
        SingleOutputStreamOperator<OrderDetail> orderDetailStream = source.map(str -> {
            List<String> orderStr = Splitter.on(",").splitToList(str);
            return new OrderDetail(orderStr.get(0), Doubles.tryParse(orderStr.get(1)),
                Longs.tryParse(orderStr.get(2)));
        })
            .assignTimestampsAndWatermarks(new MyTimeExtractor(Time.seconds(5000L)));
        DataStream<OrderDetail> userOrderStream = orderDetailStream
            .keyBy(OrderDetail::getId)
            .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .reduce(
                (o1, o2) -> new OrderDetail(o1.getId(), o1.getAmt() + o2.getAmt(), o1.getTime()),
                (key, window, ele, out) -> {
                    OrderDetail ord = ele.iterator().next();
                    System.err.println(
                        "key :" + key + " amt :" + ord.getAmt() + " time :" + ord.getTime()
                            + "  window :" + window);
                    out.collect(ord);
                },

                TypeInformation.of(OrderDetail.class)
            );
        userOrderStream
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(new ProcessAllWindowFunction<OrderDetail, OrderDetail, TimeWindow>() {
                @Override
                public void process(Context context, Iterable<OrderDetail> elements,
                    Collector<OrderDetail> out) throws Exception {
                    TreeSet<OrderDetail> orderSet = Sets
                        .newTreeSet(Comparator.comparing(OrderDetail::getAmt).reversed());
                    elements.forEach((orderDetail -> {
                        orderDetail.setId(orderDetail.getId() + " " + context.window());
                        orderSet.add(orderDetail);
                        if (orderSet.size() > 2) {
                            orderSet.pollLast();
                        }
                    }));
                    orderSet.forEach(orderDetail -> out.collect(orderDetail));
                }
            })
            .printToErr();
        env.execute("TopN Job");
    }

    static class MyTimeExtractor extends BoundedOutOfOrdernessTimestampExtractor<OrderDetail> {

        public MyTimeExtractor(
            Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(OrderDetail element) {
            return element.getTime() * 1000;
        }

    }

}
