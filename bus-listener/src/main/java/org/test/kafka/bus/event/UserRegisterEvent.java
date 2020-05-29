package org.test.kafka.bus.event;

import org.springframework.cloud.bus.event.RemoteApplicationEvent;

public class UserRegisterEvent extends RemoteApplicationEvent {
    private String username;

    public UserRegisterEvent() {
    }

    public UserRegisterEvent(Object source, String originService, String destinationService, String username) {
        super(source, originService);
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
