package com.java.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer_Test04_CallBack_Partition {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        // 配置Kafka参数
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建一个Producer对象
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        for (int i = 0; i < 20; i++) {
            kafkaProducer.send(new ProducerRecord("second", 0, "", "atguigu" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("主题：" + recordMetadata.topic() + "分区：" + recordMetadata.partition());
                    }
                }
            });
            Thread.sleep(2);
        }

        // 关闭资源
        kafkaProducer.close();
    }
}
