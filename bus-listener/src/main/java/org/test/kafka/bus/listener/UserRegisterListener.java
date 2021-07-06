package org.test.kafka.bus.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.test.kafka.bus.event.UserRegisterEvent;

@Component
public class UserRegisterListener implements ApplicationListener<UserRegisterEvent> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void onApplicationEvent(UserRegisterEvent event) {
        logger.info("[onApplicationEvent][监听到用户({}) 注册]", event.getUsername());
    }
}
