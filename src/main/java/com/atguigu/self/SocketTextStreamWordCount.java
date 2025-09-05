package com.atguigu.self;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author hb30496
 * @description: SocketTextStreamWordCount
 * @date 2024-10-12
 */

public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        // get input data
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
        env.setParallelism(2);

        eventDataStreamSource.flatMap(new ClickMap())
                .setParallelism(1)
                .keyBy(0)
                .map(t -> t.f0 + " " + System.currentTimeMillis())
                .setParallelism(2)
                .print();

        // execute program
        env.execute("Java WordCount from SocketTextStream Example");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class ClickMap implements FlatMapFunction<Event, Tuple2<String, Integer>> {
        @Override
        public void flatMap(Event value, Collector<Tuple2<String, Integer>> out) {
            String user = value.user;
            out.collect(Tuple2.of(user, 1));
        }
    }
}
