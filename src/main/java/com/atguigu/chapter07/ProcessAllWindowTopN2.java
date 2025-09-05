package com.atguigu.chapter07;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

public class ProcessAllWindowTopN2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );

        // 只需要url就可以统计数量，所以转换成String直接开窗统计
        eventStream
                .map(data -> data.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))    // 开滑动窗口
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult())
                .print();

        env.execute();
    }

    static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Long>, List<Tuple2<String, Long>>> {

        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
            accumulator.put(value, accumulator.getOrDefault(value, 0L) + 1);
            return accumulator;
        }

        @Override
        public List<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            List<Tuple2<String, Long>> result = new ArrayList<>();
            for (String url : accumulator.keySet()) {
                result.add(Tuple2.of(url, accumulator.get(url)));
            }

            result.sort((o1, o2) -> o2.f1.compareTo(o1.f1));
            return Collections.emptyList();
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            return null;
        }
    }

    static class UrlAllWindowResult extends ProcessAllWindowFunction<List<Tuple2<String, Long>>, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<List<Tuple2<String, Long>>, String, TimeWindow>.Context context, Iterable<List<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {

        }
    }
}

