package com.atguigu.self;

/**
 * @author tangibleHe-19
 * @description:
 * @date 2025-09-05
 */

import com.atguigu.chapter07.KafkaConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * <p>验证新增 kafka source算子，union到已有算子后，状态丢失问题
 * <li>保存状态需设置参数
 *  <ol>CheckpointingOptions.CHECKPOINT_STORAGE，CheckpointingOptions.CHECKPOINTS_DIRECTORY</ol>
 *  <ol>打开注释启用checkpoint</ol>
 * </li>
 * <li>从状态恢复设置参数：execution.savepoint.path，execution.savepoint.ignore-unclaimed-state</li>
 */
public class OperatorTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        boolean ck = false;
        if (ck) {
            configuration.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
            configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/hb30496/tools/flink/OperatorTest/checkpoint");
        } else {
            configuration.setString("execution.savepoint.path", "file:///Users/hb30496/tools/flink/OperatorTest/checkpoint/edddf6c257b2c8d992e011372ab6a40f/chk-8");
            configuration.setBoolean("execution.savepoint.ignore-unclaimed-state", true);
        }

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 启用checkpoint
        if (ck) {
            env.enableCheckpointing(5000);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        }

        ParameterTool argTools = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(argTools);

        // attendance: bos_t_work_attendance_topic
        FlinkKafkaConsumer atte_consumer = KafkaConfig.getConsumerFatCn("bos_t_work_attendance_topic");
        atte_consumer.setStartFromGroupOffsets();
        DataStream<String> atte_source = env.addSource(atte_consumer)
                .name("atte_kafka-source")
                .process(new ProcessFunction<String, String>() {

                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(value);
                    }
                })
                .name("atte_map");

        // task: metrics-t-task-life-cycle
        FlinkKafkaConsumer consumer_task = KafkaConfig.getConsumerFatCn("metrics-t-task-life-cycle");
        consumer_task.setStartFromGroupOffsets();
        DataStream<String> task_source = env.addSource(consumer_task)
                .name("task-kafka-source-new")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, ProcessFunction.Context ctx, Collector out) throws Exception {
                        out.collect(value);
                    }
                })
                .name("task-map");

        // track: bos_user_track_position_kafka_topic
        FlinkKafkaConsumer track_consumer = KafkaConfig.getConsumerFatCn("bos_user_track_position_kafka_topic");
        track_consumer.setStartFromGroupOffsets();
        DataStream<String> track_source = env.addSource(track_consumer)
                .name("track-kafka-source")
                .uid("track-kafka-source")
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, ProcessFunction.Context ctx, Collector out) throws Exception {
                        out.collect(value);
                    }
                })
                .name("track-map")
                .uid("track-map");

//        DataStream<String> union = atte_source.union(task_source);
        DataStream<String> union = track_source.union(atte_source).union(task_source);
        union.print();
        // 启动Flink任务
        env.execute("Flink Kafka State Example");
    }
}
