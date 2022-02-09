package com.alibaba.c2m.module02.state;

import java.util.Objects;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.StateTtlConfig.StateVisibility;
import org.apache.flink.api.common.state.StateTtlConfig.UpdateType;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author qingyong
 * @date 2022/02/09
 */
public class StateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 5L),
            Tuple2.of(1L, 2L))
            .keyBy(0)
            .flatMap(new CountWindowAverage())
            .printToErr();
        env.execute("count average");
    }

    public static class CountWindowAverage extends
        RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out)
            throws Exception {
            Tuple2<Long, Long> currentSum;
            if (Objects.isNull(sum.value())) {
                currentSum = Tuple2.of(0L, 0L);
            } else {
                currentSum = sum.value();
            }
            currentSum.f0 += 1;
            currentSum.f1 += value.f1;
            sum.update(currentSum);

            if (currentSum.f0 >= 2) {
                out.collect(Tuple2.of(currentSum.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "average",
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                }));
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                .setUpdateType(UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateVisibility.NeverReturnExpired)
                .build();
            descriptor.enableTimeToLive(ttlConfig);
            sum = getRuntimeContext().getState(descriptor);
        }

    }


}
