package org.test.kafka.bus.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.test.kafka.bus.event.UserRegisterEvent;

@RestController
@RequestMapping("/bus")
public class BusController {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    @Autowired
    private ServiceMatcher busServiceMatcher;

    @GetMapping("/register")
    public boolean register(String username) {
        logger.info("[register][执行用户({}) 的注册逻辑]", username);

        applicationEventPublisher.publishEvent(
                new UserRegisterEvent(this, busServiceMatcher.getServiceId(), null, username));
        return true;
    }
}
