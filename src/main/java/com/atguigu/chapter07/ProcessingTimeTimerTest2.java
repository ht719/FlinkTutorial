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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

public class ProcessingTimeTimerTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 处理时间语义，不需要分配时间戳和watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        KeyedProcessFunction<Boolean, Event, String> keyedProcessFunction = new KeyedProcessFunction<Boolean, Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                Long currTs = ctx.timerService().currentProcessingTime();
                out.collect("数据到达，到达时间：" + new Timestamp(currTs));
                // 注册一个10秒后的定时器
                ctx.timerService().registerProcessingTimeTimer(currTs + 1000L);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));

                long sysTime = System.currentTimeMillis();
                out.collect(String.format("sysTime: %s, timestamp: %s", new Timestamp(sysTime), new Timestamp(timestamp)));
            }
        };
        // 要用定时器，必须基于KeyedStream
        stream.keyBy(data -> true)
                .process(keyedProcessFunction)
                .print();

        stream.keyBy(data -> true)
                .process(keyedProcessFunction)
                .print();

        env.execute();
    }
}

