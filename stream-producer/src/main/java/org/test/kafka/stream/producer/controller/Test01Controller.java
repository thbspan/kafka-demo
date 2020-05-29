package org.test.kafka.stream.producer.controller;

import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.test.kafka.stream.producer.message.MySource;
import org.test.kafka.stream.producer.message.Test01Message;

@RestController
@RequestMapping("/test01")
public class Test01Controller {

    @Autowired
    private MySource source;

    @GetMapping("/send")
    public boolean send() {
        Test01Message message = new Test01Message();
        message.setId(new Random().nextInt());

        Message<Test01Message> myMessage = MessageBuilder.withPayload(message).build();

        return source.test01Output().send(myMessage);
    }

    @GetMapping("/send_orderly")
    public boolean sendOrderly() {
        // 发送 3 条相同 id 的消息
        int id = new Random().nextInt();
        for (int i = 0; i < 3; i++) {
            Test01Message message = new Test01Message();
            message.setId(id);
            // 创建 Spring Message 对象
            Message<Test01Message> springMessage = MessageBuilder.withPayload(message)
                    .build();
            // 发送消息
            source.test01Output().send(springMessage);
        }
        return true;
    }

    @GetMapping("/send_tag")
    public boolean sendTag() {
        for(String tag : new String[] {"human", "animal", "plant"}) {
            Test01Message message = new Test01Message();
            message.setId(new Random().nextInt());
            // 创建 Spring Message 对象
            Message<Test01Message> springMessage = MessageBuilder.withPayload(message)
                    .setHeader("tag", tag)
                    .build();
            // 发送消息
            source.test01Output().send(springMessage);
        }

        return true;
    }
}
