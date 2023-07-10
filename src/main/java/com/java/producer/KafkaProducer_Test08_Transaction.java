package com.java.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer_Test08_Transaction {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        // 配置Kafka参数
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 自定义分区
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.java.producer.KafkaProcucer_MyPartition_Test01");

        // 设置批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        // Linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "2");
        // 压缩:   gizp、snappy、lz4、zstd；通常是snappy
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        // 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        // acks: 生产上一般用1； 或者-1（涉及钱或者高度准确的场景）；  精准一次消费：幂等性 + ack=-1 + ISR>=2 + 副本>=2
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");

        // Retry: 默认值为INT的最大值
        properties.put(ProducerConfig.RETRIES_CONFIG, "5");

        // 开启幂等性:默认开启     开启幂等性，可以分区内有序：PID + 分区号 + 序列号； 下一个数据没有之前其他来的数据保存，等下一个来；
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);

        // 初始化全局唯一PID
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,KafkaProducer_Test08_Transaction.class.getName());

        // 创建一个Producer对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);

        // 初始化事务：使用事务必须初始化一个全局唯一PID
        kafkaProducer.initTransactions();
        // 开启事务
        kafkaProducer.beginTransaction();

        try {
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord("second", "hello" + i), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println("主题：" + recordMetadata.topic() + "分区：" + recordMetadata.partition());
                        }
                    }
                });
                Thread.sleep(2);
            }
            // 提交事务
            kafkaProducer.commitTransaction();
        } catch (InterruptedException e) {
            kafkaProducer.abortTransaction();
            throw new RuntimeException(e);
        } finally {
            // 关闭资源
            kafkaProducer.close();
        }


    }
}
