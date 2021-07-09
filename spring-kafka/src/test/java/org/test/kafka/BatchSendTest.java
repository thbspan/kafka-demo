package org.test.kafka;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * 测试批量发送
 */
@TestPropertySource(properties = {
        "spring.kafka.producer.acks=1",
        "spring.kafka.producer.batch-size=16384",
        "spring.kafka.producer.buffer-memory=33554432",
        "spring.kafka.producer.properties.linger.ms=1000"})
public class BatchSendTest extends SpringTestBase {
    @Resource
    private SimpleProducer simpleProducer;

    @Test
    public void test() throws InterruptedException {
        int count = 5;
        CountDownLatch countDownLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            simpleProducer.asyncSend(ThreadLocalRandom.current().nextInt(300, 10000)).addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onFailure(Throwable ex) {
                    logger.error("send exception", ex);
                    countDownLatch.countDown();
                }

                @Override
                public void onSuccess(SendResult<String, String> result) {
                    logger.error("send message {} success", result);
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
    }
}
