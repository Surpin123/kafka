package com.java.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducer_Test03_Get {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();

        // 配置Kafka参数
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建一个Producer对象
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord("second", "atguigu" + i)).get();
        }

        // 关闭资源
        kafkaProducer.close();
    }
}
