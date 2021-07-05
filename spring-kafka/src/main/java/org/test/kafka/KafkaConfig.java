package org.test.kafka;

import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Configuration
public class KafkaConfig {

    @Bean
    public ApplicationRunner applicationRunner(KafkaTemplate<String, String> kafkaTemplate) {
        return args -> new Thread(() -> {
            // 发送消息
            for (int i = 10; i < 20; i++) {
                ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send("topic-test", Integer.toString(i), "test data " + i);
                send.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

                    @Override
                    public void onSuccess(SendResult<String, String> result) {
                        System.out.println("send meg=" + result.getProducerRecord() + " success");
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });
            }
        }).start();
    }
}
