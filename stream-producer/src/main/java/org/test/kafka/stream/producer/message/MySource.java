package org.test.kafka.stream.producer.message;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface MySource {

    @Output("test01-output")
    MessageChannel test01Output();
}
