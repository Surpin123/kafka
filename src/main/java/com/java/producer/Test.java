package com.java.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        // 0. 配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 优化：batch，linger.ms，buffer，compress
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "14324");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "523452");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // acks，retry
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "5");

        // 自定义分区
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.java.producer.TestPartition");


        // 1. 创建生产者
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 2. 生产数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("second", 1, "key", "atguigu123"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.out.println("主题：" + recordMetadata.topic() + "，分区：" + recordMetadata.partition());
                    }
                }
            });
            Thread.sleep(1000);
        }

        // 3. 关闭资源
        kafkaProducer.close();
    }
}
