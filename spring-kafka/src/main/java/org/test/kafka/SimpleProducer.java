package org.test.kafka;

import java.util.concurrent.ExecutionException;

import javax.annotation.Resource;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class SimpleProducer {

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    public SendResult<String, String> syncSend(Integer id) throws ExecutionException, InterruptedException {
        return kafkaTemplate.send(SimpleConsumerListener.TOPIC, String.valueOf(id)).get();
    }

    public ListenableFuture<SendResult<String, String>> asyncSend(Integer id) {
        return kafkaTemplate.send(SimpleConsumerListener.TOPIC, String.valueOf(id));
    }
}
