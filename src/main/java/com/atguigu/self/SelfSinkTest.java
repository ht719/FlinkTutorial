package com.atguigu.self;

import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.OutputTag;

import java.util.*;

/**
 * @author hb30496
 * @description: test
 * @date 2024-12-29
 */

public class SelfSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度为1
        env.setParallelism(1);

        Event[] event = new Event[]{
                new Event("Mary", "./home", 1000L),
                new Event("Mary", "./cart", 2000L),
                new Event("Mary", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L)
        };

        DataStream<Event> source =  env.fromElements(event);
//        DataStream<Event> source = env.addSource(new ClickSource());

        // 添加侧输出
        OutputTag<String> outputTag = new OutputTag<String>("side-output-test"){};

        source.keyBy(data -> data.user)
                .addSink(new RichSinkFunction<Event>() {
                    Map<String, String> map = new HashMap<>();
                    ValueState<String> valueState;
                    ValueState<String> keyState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("value", String.class));
                        keyState = getRuntimeContext().getState(new ValueStateDescriptor<>("key", String.class));
                    }

                    @Override
                    public void invoke(Event value, Context context) throws Exception {
                        valueState.update(value.url);
                        keyState.update(value.user);
                        map.put(keyState.value(), valueState.value());
                        System.out.println(map);
                        System.out.println("=========");
                    }
                });

        env.execute();
    }
}
