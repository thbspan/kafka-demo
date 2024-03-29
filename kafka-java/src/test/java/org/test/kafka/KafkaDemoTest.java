package org.test.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.test.partition.BananaPartitioner;

public class KafkaDemoTest {

    @Test
    public void createTopic() {
        System.out.println("createTopic starting");

        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
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
        // 完整的参数见 ConsumerConfig
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 指定消费者群组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 自动提交时间间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 分区分配策略 / RoundRobin - 会把所有主题和分区一起分配
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        // 单次调用poll返回的最大记录数量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        // session 超时时间，默认10s
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        // 心跳时间间隔
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 订阅主题、可以传入正则表达式匹配多个主题
        consumer.subscribe(Collections.singletonList("topic-test"), new ConsumerRebalanceListener() {
            /**
             * 再均衡开始之前和停止读取消息后被调用
             * 在这里提交偏移量，下一个接管分区的消费者就知道从哪里开始读取
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 提交偏移量 Map<TopicPartition, OffsetAndMetadata> currentOffsets = ...; consumer.commitSync(currentOffsets);
                consumer.commitSync();
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
        // commitSync(consumer);
        // commitAsync(consumer);
        // commitAsyncAndSync(consumer);
        commitSpecOffset(consumer);
    }

    /**
     * 同步提交当前批次最新的消息
     */
    private void commitSync(KafkaConsumer<String, String> consumer) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                // 轮询消息
                // 就像鲨鱼停止游动会死掉一样，消费者必须对kafka进行轮询，否则会被认为已经死亡，它的分区被移交给群组里的其他消费者
                // poll方法接收一个超时时间参数，指定多久之后可以返回，如果有数据立即返回，没有会等到超时后返回空列表
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d, offset = %d, key= %s value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
                /*
                 * 同步提交，提交当前批次最新的偏移量
                 * 在成功提交或碰到无法恢复的错误之前，commitSync会一直重试
                 */
                consumer.commitSync();
                // 同步和异步组合提交  consumer.commitAsync / consumer.commitSync
            }
        } catch (WakeupException e) {
            // 消费者优雅退出：调用consumer.wakeup()，poll方法会抛出WakeupException异常
        } finally {
            // 关闭消费者，并且会立即触发一次再均衡
            consumer.close();
        }
    }

    /**
     * 异步提交当前批次最新的消息
     */
    private void commitAsync(KafkaConsumer<String, String> consumer) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                // 轮询消息
                // 就像鲨鱼停止游动会死掉一样，消费者必须对kafka进行轮询，否则会被认为已经死亡，它的分区被移交给群组里的其他消费者
                // poll方法接收一个超时时间参数，指定多久之后可以返回，如果有数据立即返回，没有会等到超时后返回空列表
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d, offset = %d, key= %s value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
                /*
                 * 异步提交，提交当前批次最新的偏移量
                 * 异步提交不会执行重试
                 */
                consumer.commitAsync((offsets, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    }
                });
            }
        } catch (WakeupException e) {
            // 消费者优雅退出：调用consumer.wakeup()，poll方法会抛出WakeupException异常
        } finally {
            // 关闭消费者，并且会立即触发一次再均衡
            consumer.close();
        }
    }

    /**
     * 同步和异步组合提交
     */
    private void commitAsyncAndSync(KafkaConsumer<String, String> consumer) {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                // 轮询消息
                // 就像鲨鱼停止游动会死掉一样，消费者必须对kafka进行轮询，否则会被认为已经死亡，它的分区被移交给群组里的其他消费者
                // poll方法接收一个超时时间参数，指定多久之后可以返回，如果有数据立即返回，没有会等到超时后返回空列表
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d, offset = %d, key= %s value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
                /*
                 * 异步提交，提交当前批次最新的偏移量
                 * 异步提交不会执行重试
                 * 如果一切正常，使用异步提交，这样速度更快
                 */
                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                /*
                 * 出现异常，同步提交的方式，会一直重试，直到提交成功或发生无法恢复的错误
                 * 但是这里提交的是最后一个偏移量，可能有些消息还没有处理，导致遗漏
                 */
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    /**
     * 提交特定的偏移量
     */
    private void commitSpecOffset(KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        try {
            while (!Thread.currentThread().isInterrupted()) {
                int count = 0;
                // 轮询消息
                // 就像鲨鱼停止游动会死掉一样，消费者必须对kafka进行轮询，否则会被认为已经死亡，它的分区被移交给群组里的其他消费者
                // poll方法接收一个超时时间参数，指定多久之后可以返回，如果有数据立即返回，没有会等到超时后返回空列表
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d, offset = %d, key= %s value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                    if (count % 10 == 0) {
                        consumer.commitAsync(currentOffsets, (offsets, e) -> {
                            if (e != null) {
                                e.printStackTrace();
                            }
                        });
                    }
                    count++;
                }
                /*
                 * 异步提交，提交当前批次最新的偏移量
                 * 异步提交不会执行重试
                 * 如果一切正常，使用异步提交，这样速度更快
                 */
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            e.printStackTrace();
        } finally {
            try {
                /*
                 * 出现异常，同步提交的方式，会一直重试，直到提交成功或发生无法恢复的错误
                 */
                consumer.commitSync(currentOffsets);
            } finally {
                consumer.close();
            }
        }
    }

    @Test
    public void testProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 0  ：不应答。
        // 1  ：leader 应答。
        // all:当所有同步副本收到消息时，生产者才会收到一个来自服务器的成功应答，延迟高，可以结合 min.insync.replicas 参数使用
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // 发送失败时，重试发送的次数
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 重试的时间间隔
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        // 一个批次可以使用的内存大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 在发送批次之前最长的等待时间，会增加延迟，但能够提升吞吐量
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        // 启用发送消息压缩
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        /*
         * 生产者内存缓冲区的大小，生产者用它缓冲要发送到服务器的消息
         * 如果应用程序发送消息的速度 > 发送到服务器的速度，会导致缓冲区空间不足，
         * 这个时候调用send()方法，要么阻塞，要么抛出异常，取决于 MAX_BLOCK_MS_CONFIG(max.block.ms)参数
         * （表示在抛出异常之前可以阻塞一会儿）
         * 单位 bytes
         * 33554432 bytes = 32768 KB (*1024) = 32 MB (*1024*1024)
         */
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 自定义分区策略
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, BananaPartitioner.class.getName());
        // 同步发送
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // key 值可以设置，也可以不设置；相同的key值会发送到相同的分区
            producer.send(new ProducerRecord<>("topic-test", null, Integer.toString(20))).get();
        } catch (ExecutionException | InterruptedException e) {
            /*
             * 如果在发送数据之前或者发送过程中发生任何错误，比如broker返回一个不容许重发消息的异常或已经超过了重发的次数，
             * 那么就会抛出异常
             * 错误一般分为可重试错误和不可重试错误
             * 可重试错误：连接错误、无主错误；多次重试还是异常，会收到 重试异常 错误
             * 不可重试错误：消息太大
             */
            e.printStackTrace();
        }
        // 异步发送
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 100; i < 120; i++) {
                producer.send(new ProducerRecord<>("topic-test", null, Integer.toString(i)), (recordMetadata, e) -> {
                    // 回调处理
                    if (e != null) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }
}
