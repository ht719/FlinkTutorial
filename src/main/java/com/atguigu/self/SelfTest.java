package com.atguigu.self;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import kafka.server.DelayedFuture;
import kafka.utils.timer.SystemTimer;
import kafka.utils.timer.TimerTask;
import kafka.utils.timer.TimerTaskEntry;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import scala.collection.Seq;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * @author hb30496
 * @description: selfTest
 * @date 2024-10-03
 */

public class SelfTest {
    public static void main(String[] args) throws Exception {
        flinkTest2();
    }

    private static void flinkTest2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .name("source-1")
                .uid("uid-source-1")
                .process(new ProcessFunctionTest())
                .setParallelism(2)
                .name("process-1")
                .uid("uid-process-1")
                .print()
                .name("print-1")
                .uid("uid-print-1");

        env.execute();
    }

    private static void flinkTst() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "ht-test-group");

        FlinkKafkaConsumer<String> source1 = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);

        SingleOutputStreamOperator<String> commonSource = env.addSource(source1)
                .name("name-source-1")
                .uid("uid-source-1");
        commonSource
//                .filter((FilterFunction<String>) value -> true)
//                .name("name-flatMap-1")
//                .uid("uid-flatMap-1")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(value);
                    }
                })
                .name("name-process-1")
                .uid("uid-process-1")
                .keyBy(data -> data.toLowerCase())
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, KeyedProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toLowerCase());
                    }
                })
                .name("name-keyed-process-1")
                .uid("name-keyed-process-1")
                .print();


        env.execute();
    }

    private static class ProcessFunctionTest extends ProcessFunction<Event, String> {

        @Override
        public void processElement(Event event, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
            int a = 1/0;
            out.collect(event.url);
        }
    }
}
