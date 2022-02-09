package com.alibaba.c2m.module02.watermark;

import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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
            .keyBy(0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .minBy(1)
            .print();

        env.execute("EventTime Watermark");


    }

    public static class MyTimestampExtractor implements AssignerWithPeriodicWatermarks<String> {

        private Long currentTimeStamp = 0L;

        private Long maxOutOfOrderness = 0L;

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
