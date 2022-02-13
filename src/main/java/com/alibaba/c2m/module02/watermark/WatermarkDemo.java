package com.alibaba.c2m.module02.watermark;

import com.google.common.base.Joiner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * @author qingyong
 * @date 2022/02/09
 */
public class WatermarkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100L);

        env.socketTextStream("127.0.0.1", 9000)
            .assignTimestampsAndWatermarks(new MyTimestampExtractor())
            .map(str -> {
                String[] eleAndTime = str.split(",");
                return new Tuple2<>(eleAndTime[0], Long.parseLong(eleAndTime[1]));
            }, TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
            }))
            .keyBy(t -> t.f0)
            .process(new KeyedProcessFunction<String,Tuple2<String, Long>, Tuple2<String, Long>>() {

                @Override
                public void processElement(Tuple2<String, Long> value, Context ctx,
                    Collector<Tuple2<String, Long>> out) throws Exception {
                    ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 1000L);
                    out.collect(value);
                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx,
                    Collector<Tuple2<String, Long>> out) throws Exception {
                    System.err.println("Ontimer : " + timestamp);
                }
            })
//            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//            .minBy(1)
//            .aggregate(new MyAggregateFn(), new MyProcessWindowFn())
            .print();

        env.execute("EventTime Watermark");

    }

    public static class MyAggregateFn implements
        AggregateFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> createAccumulator() {
            return new Tuple2<>("", Long.MIN_VALUE);
        }

        @Override
        public Tuple2<String, Long> add(Tuple2<String, Long> value,
            Tuple2<String, Long> accumulator) {
            accumulator.f0 = value.f0;
            accumulator.f1 = Math.min(value.f1, accumulator.f1);
            return accumulator;
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> a,
            Tuple2<String, Long> b) {
            long min = Math.min(a.f1, b.f1);
            return new Tuple2<>(a.f0, min);
        }

    }

    public static class MyProcessWindowFn extends
        ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements,
            Collector<String> out) throws Exception {
            String eles = Joiner.on(",").join(elements);
            out.collect("Key :" + s + " Window :" + context.window() + " elements :" + eles);
        }

    }

    public static class MyTimestampExtractor implements AssignerWithPeriodicWatermarks<String> {

        private Long currentTimeStamp = 0L;

        private Long maxOutOfOrderness = 5000L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimeStamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            String[] arr = element.split(",");
            long timeStamp = Long.parseLong(arr[1]) * 1000L;
            currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
            System.err.println(
                element + ",EventTime:" + timeStamp + ",watermark:" + (currentTimeStamp
                    - maxOutOfOrderness));
            return timeStamp;
        }

    }

}
