package com.atguigu.chapter07;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class KafkaConfig {

    private static Producer<String, String> producer;

    public static void init() {


    }

    public static FlinkKafkaConsumer getConsumer(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "uat-kafka-002-sg01-svr1.ttbike.com.cn:9092,uat-kafka-002-sg01-svr2.ttbike.com.cn:9092,uat-kafka-002-sg01-svr3.ttbike.com.cn:9092");
        props.put("group.id", "flink-label-engine-new");
        props.put("flink.partition-discovery.interval-millis", 10000);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);

        return flinkKafkaConsumer;
    }

    public static Producer<String, String> getProducer() {
        if (producer == null) {
            init();
        }
        return producer;
    }

    public static void close() {
        if (producer != null) {
            producer.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
//        int num = 1000;
//        CountDownLatch countDownLatch = new CountDownLatch(num);
//        // 创建多个线程调用getProducer方法
//        // 当前标准时间,转换为yyyy MM dd HH mm ss
//        for (int i = 0; i < num; i++) {
//            int finalI = i;
//            new Thread(() -> {
//                Producer<String, String> producer1 = getProducer();
//                System.out.println(Thread.currentThread().getName() + " " + producer1);
//                // 发送消息
//                producer1.send(new ProducerRecord<>("test", "hello"));
//                countDownLatch.countDown();
//            }, "T" + finalI).start();
//        }
//        countDownLatch.await();

        // 发送消息
        // producer.send(new ProducerRecord<>("test", "hello"));

        Scanner in = new Scanner(System.in);
        int i = in.nextInt();

        Producer<String, String> producer = getProducer();
        producer.send(new ProducerRecord<>("test", "hello"));
    }

}
