package org.test.kafka;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class SimpleProducerTest extends SpringTestBase {

    @Resource
    private SimpleProducer simpleProducer;

    @Test
    public void testSyncSend() throws ExecutionException, InterruptedException {
        SendResult<String, String> result = simpleProducer
                .syncSend(ThreadLocalRandom.current().nextInt(300, 10000));
        logger.info("send message {}", result);
    }

    @Test
    public void testASyncSend() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        simpleProducer.asyncSend(ThreadLocalRandom.current().nextInt(300, 10000))
                .addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
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
        countDownLatch.await();
    }
}
