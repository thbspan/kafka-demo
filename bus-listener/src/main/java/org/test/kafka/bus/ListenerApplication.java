package org.test.kafka.bus;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.bus.jackson.RemoteApplicationEventScan;

@SpringBootApplication
@RemoteApplicationEventScan
public class ListenerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ListenerApplication.class);
    }
}
