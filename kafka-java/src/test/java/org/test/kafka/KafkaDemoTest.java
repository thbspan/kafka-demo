package org.test.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

public class KafkaDemoTest {

    @Test
    public void createTopic() {
        System.out.println("createTopic starting");

        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        AdminClient adminClient = AdminClient.create(props);

        CreateTopicsResult createResult = adminClient.createTopics(Collections
                .singletonList(new NewTopic("topic-test", 1, (short) 1)));

        try {
            createResult.all().get();
            System.out.println("createTopic end");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("topic-test"), new ConsumerRebalanceListener() {
            /**
             * 这个方法里面可以提交偏移量操作以避免数据重复消费
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 提交偏移量
                consumer.commitAsync();
            }

            /**
             * 这个方法在平衡之后、消费者开始去拉取消息之前被调用，
             * 一般在该方法中保证各消费者回滚到正确的偏移量，即重置各消费者消费偏移量
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 获取该分区下已消费的偏移量
                Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(partitions), Duration.ofMillis(60_000));
                for (TopicPartition topicPartition : partitions) {
                    // 获取该分区下已消费的偏移量

                    // 重置偏移量到上一次提交的偏移量下一个位置处开始消费
                    OffsetAndMetadata offsetAndMetadata = committed.get(topicPartition);
                    if (offsetAndMetadata != null) {
                        consumer.seek(topicPartition, offsetAndMetadata.offset() + 1);
                    }
                }

                // consumer.seekToBeginning(partitions);
            }
        });

        while (!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d, offset = %d, key= %s value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    @Test
    public void testProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        // 0-不应答。1-leader 应答。all-所有 leader 和 follower 应答
        props.put("acks", "1");
        // 发送失败时，重试发送的次数
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>("topic-test", Integer.toString(i), Integer.toString(i)));
            }
        }
    }
}
