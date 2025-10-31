package com.atguigu.self;

import com.alibaba.fastjson.JSON;
import com.atguigu.chapter07.HeartEntity;
import com.atguigu.chapter07.KafkaConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author hb30496
 * @description: kafka test
 * @date 2024-09-28
 */

/**
 * 验证新增kafka消费者source，点位重置问题
 */
public class KafkaTest {
    // 写一段代码，从kafka中读取数据，并打印出来。
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
//        configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/hb30496/tools/flink/checkpoint");
        // /4143a96046d7e5ef25266278259fd9e9/chk-2 只有老 topic 状态
        // /410f39c7c4d06c156e2f427d9b47aa77/chk-4 新老 topic 状态都有
        // /061c986d19612ae413ba794f68ff7727/chk-9 新老 topic 状态都有
        // /8bc4e3cdbc67b2a6e43b7a4d8cb4ba10/chk-13 新老 topic union
         configuration.setString("execution.savepoint.path", "file:///Users/hb30496/tools/flink/checkpoint/061c986d19612ae413ba794f68ff7727/chk-9");
        configuration.setBoolean("execution.savepoint.ignore-unclaimed-state", true);

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 启用checkpoint
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        ParameterTool argTools = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(argTools);

        // sg_lock_heart_msg_topic
        FlinkKafkaConsumer consumer = KafkaConfig.getConsumer("sg_tw_common_com_lock_heart_report_topic");
        consumer.setStartFromGroupOffsets();
        DataStream<String> stream = env.addSource(consumer)
                .uid("kafka-source-new")
                .name("kafka-source-new");

        // sg_tw_common_com_lock_heart_report_topic
//        FlinkKafkaConsumer consumer_new = KafkaConfig.getConsumer("sg_tw_common_com_lock_heart_report_topic");
//        consumer_new.setStartFromGroupOffsets();
//        DataStream<String> stream_old = env.addSource(consumer_new)
//                .uid("kafka-source-new")
//                .name("kafka-source-new");
//        stream_old.print().uid("print-new").name("print-new");

        // DataStream<String> union = stream.union(stream_old);
        DataStream<String> union = stream;

        SingleOutputStreamOperator<HeartEntity> heart = union.map(new MapFunction<String, HeartEntity>() {
            @Override
            public HeartEntity map(String value) throws Exception {
                HeartEntity heartEntity = JSON.parseObject(value, HeartEntity.class);
                return heartEntity;
            }
        }).uid("map-heart").name("map-heart");

        // 使用状态计数
        DataStream<Long> countStream = heart.keyBy(HeartEntity::getCommandNo)
                .map(new RichMapFunction<HeartEntity, Long>() {
                    private transient ValueState<Long> countState;
                    private long count = 0;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态
                        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("count-state", TypeInformation.of(Long.class));
                        countState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Long map(HeartEntity value) throws Exception {
                        count = countState.value() == null ? 0 : countState.value();
                        countState.update(++count);
                        System.out.println("commandNo: " + value.getCommandNo() + " count: " + count);
                        return countState.value();
                    }
                })
                .uid("count-map")
                .name("count-map");

        // 打印计数结果
        countStream.print().uid("print").name("print");

        // 启动Flink任务
        env.execute("Flink Kafka State Count Example");

    }

}
