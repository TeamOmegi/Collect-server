package org.omegi.test;

import java.util.NoSuchElementException;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MyService {
    private static final Logger logger = LoggerFactory.getLogger(MyService.class);
    public void testMethod1() throws Exception {
        logger.info("In TestMethod1");
    }

    public void testMethod2() throws Exception {
        logger.info("In TestMethod2");
        testMethod3();
    }

    private void testMethod3() throws Exception {
        logger.info("In TestMethod3");
    }
}