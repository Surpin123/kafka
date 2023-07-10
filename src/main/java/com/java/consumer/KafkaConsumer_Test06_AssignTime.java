package com.java.consumer;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public class KafkaConsumer_Test06_AssignTime {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop012:9092,hadoop103:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "2");

        // 手动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        // 自动提交的时间
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,1000);


        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 订阅主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("second");
        kafkaConsumer.subscribe(topics);

        // 指定消费的offset     TopicPartition：（Topic，Partition）
        Set<TopicPartition> assignment = kafkaConsumer.assignment();

        System.out.println(assignment);
        // 获取分区分配方案
        while (assignment.size() == 0){
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment = kafkaConsumer.assignment();
            System.out.println(assignment);
        }

        for (TopicPartition topicPartition : assignment) {
            kafkaConsumer.seek(topicPartition,200);
        }


        // 消费数据
        while (true) {
            ConsumerRecords<String,String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String,String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
            // 手动提交offset
            //异步: 生产上通常是异步
            kafkaConsumer.commitAsync();

/*            // 同步
            kafkaConsumer.commitSync();*/
        }
    }
}
