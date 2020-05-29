package org.test.kafka.stream.consumer.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.stereotype.Component;
import org.test.kafka.stream.consumer.message.Test01Message;

@Component
public class Test01Consumer {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @StreamListener(value = MySink.TEST01_INPUT, condition = "headers['tag'] == 'animal'")
    public void onMessage(@Payload Test01Message message) {
        logger.info("[onMessage][Thread Id:{} Content：{}]", Thread.currentThread().getId(), message);

//        throw new RuntimeException("throw exception for test");
    }

    /**
     * 局部异常处理
     */
    @ServiceActivator(inputChannel = "test-01.test-group.errors")
    public void handleError(ErrorMessage errorMessage) {
        logger.error("[handleError][payload：{}]", errorMessage.getPayload().getMessage());
        logger.error("[handleError][originalMessage：{}]", errorMessage.getOriginalMessage());
        logger.error("[handleError][headers：{}]", errorMessage.getHeaders());
    }

    /**
     * 默认的全局异常处理
     */
    @StreamListener(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME)
    public void globalHandleError(ErrorMessage errorMessage) {
        logger.error("[globalHandleError][payload：{}]", errorMessage.getPayload().getMessage());
        logger.error("[globalHandleError][originalMessage：{}]", errorMessage.getOriginalMessage());
        logger.error("[globalHandleError][headers：{}]", errorMessage.getHeaders());
    }
}
