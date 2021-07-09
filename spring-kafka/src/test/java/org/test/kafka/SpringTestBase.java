package org.test.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class SpringTestBase {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
}
