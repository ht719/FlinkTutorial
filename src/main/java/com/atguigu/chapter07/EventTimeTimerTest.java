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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Calendar;

public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8081);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(2);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 基于KeyedStream定义事件时间定时器
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value + " 时间戳为：" + new Timestamp(ctx.timestamp()) + " 水位线为：" + ctx.timerService().currentWatermark());
                        // 注册一个定时器
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 1500L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 定时器触发，触发时间：" + new Timestamp(timestamp) + " 当前时间: " + new Timestamp(Calendar.getInstance().getTimeInMillis()));
                    }
                })
                .print();

        env.execute();
    }

    // 自定义测试数据源
    public static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            long timeInMillis = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event("Mary", "./home", timeInMillis));
            ctx.collect(new Event("Bob", "./home", Calendar.getInstance().getTimeInMillis()));
            ctx.collect(new Event("Bob", "./home", Calendar.getInstance().getTimeInMillis()));
            ctx.collect(new Event("Bob", "./home", Calendar.getInstance().getTimeInMillis()));

            // 直接发出测试数据
//            ctx.collect(new Event("Mary", "./home", timeInMillis));
//            Thread.sleep(1000);
//            ctx.collect(new Event("Bob", "./cart", timeInMillis + 500L));
//            Thread.sleep(1000);
//            ctx.collect(new Event("Bob", "./cart", timeInMillis + 800L));
////            // 为了更加明显，中间停顿5秒钟
//            Thread.sleep(1000L);
//            ctx.collect(new Event("Mary", "./cart", timeInMillis + 3000L));
//            Thread.sleep(1000L);
//            ctx.collect(new Event("Bob", "./cart", timeInMillis + 800L));
//            Thread.sleep(3000L);
//            // 发出10秒后的数据
//            ctx.collect(new Event("Mary", "./home", 11000L));
//            Thread.sleep(5000L);
//
//            // 发出10秒+1ms后的数据
//            ctx.collect(new Event("Alice", "./cart", 11001L));
//            Thread.sleep(5000L);
        }

        @Override
        public void cancel() {
        }
    }
}

