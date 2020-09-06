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
import org.apache.kafka.common.errors.WakeupException;
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
        // 指定消费者群组
        props.put("group.id", "test");
        // 自动提交
        props.put("enable.auto.commit", "true");
        // 自动提交时间间隔
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        // 分区分配策略 / RoundRobin - 会把所有主题和分区一起分配
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        // 单次调用poll返回的最大记录数量
        props.put("max.poll.records", 100);
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题、可以传入正则表达式匹配多个主题
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
        final Thread mainThread = Thread.currentThread();
        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Starting exit...");
            // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
            consumer.wakeup();
            try {
                // 等待主线程执行完成
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
        try {
            while (!Thread.currentThread().isInterrupted()) {
                // 轮询消息
                // 就像鲨鱼停止游动会死掉一样，消费者必须对kafka进行轮询，否则会被认为已经死亡，它的分区被移交给群组里的其他消费者
                // poll方法接收一个超时时间参数，指定多久之后可以返回，如果有数据立即返回，没有会等到超时后返回空列表
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d, offset = %d, key= %s value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (WakeupException e) {
            // 消费者优雅退出：调用consumer.wakeup()，poll方法会抛出WakeupException异常
        } finally {
            // 关闭消费者，并且会立即触发一次再均衡
            consumer.close();
        }

    }

    @Test
    public void testProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        // 0  ：不应答。
        // 1  ：leader 应答。
        // all:当所有参考复制的节点全部收到消息时，生产者才会收到一个来自服务器的成功应答，延迟高
        props.put("acks", "1");
        // 发送失败时，重试发送的次数
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        // 启用发送消息压缩
        props.put("compression.type", "snappy");
        // 生产者内存缓冲区的大小，生产者用它缓冲要发送到服务器的消息
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100; i++) {
                producer.send(new ProducerRecord<>("topic-test", Integer.toString(i), Integer.toString(i)), (recordMetadata, e) -> {
                    // 回调处理
                    if (e != null) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }
}
