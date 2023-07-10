package com.java.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer_Test07_Acks {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        // 配置Kafka参数
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 自定义分区
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.java.producer.KafkaProcucer_MyPartition_Test01");

        // 设置批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        // Linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"2");
        // 压缩:   gizp、snappy、lz4、zstd；通常是snappy
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        // 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");

        // acks: 生产上一般用1； 或者-1（涉及钱或者高度准确的场景）；  精准一次消费：幂等性 + ack=-1 + ISR>=2 + 副本>=2
        properties.put(ProducerConfig.ACKS_CONFIG,"1");

        // Retry: 默认值为INT的最大值
        properties.put(ProducerConfig.RETRIES_CONFIG,"5");

        // 创建一个Producer对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);

        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord("second",  "hello" + i), new Callback() {
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
