package org.test.kafka.stream.consumer.listener;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;

public interface MySink {
    String TEST01_INPUT = "test01-input";

    @Input(TEST01_INPUT)
    SubscribableChannel test01Input();
}
