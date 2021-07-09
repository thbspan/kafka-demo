package org.test.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class SimpleConsumerListener {

    public static final String TOPIC = "topic-test";
    /**
     * https://www.jianshu.com/p/a64defb44a23
     * <br/>
     * listen1(String data)
     *
     * listen2(ConsumerRecord<K,V> data)
     *
     * listen3(ConsumerRecord<K,V> data, Acknowledgment acknowledgment)
     *
     * listen4(ConsumerRecord<K,V> data, Acknowledgment acknowledgment, Consumer<K,V> consumer)
     *
     * listen5(List<String> data)
     *
     * listen6(List<ConsumerRecord<K,V>> data)
     *
     * listen7(List<ConsumerRecord<K,V>> data, Acknowledgment acknowledgment)
     *
     * listen8(List<ConsumerRecord<K,V>> data, Acknowledgment acknowledgment, Consumer<K,V> consumer)
     */
    @KafkaListener(topics = {TOPIC})
    public void listen(ConsumerRecord<String, String> record) {
        System.out.println("rec message: " + record);
    }
}
