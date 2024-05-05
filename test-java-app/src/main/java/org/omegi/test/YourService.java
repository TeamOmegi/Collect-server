package org.omegi.test;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class YourService {
    private static final Logger logger = LoggerFactory.getLogger(MyService.class);
    public void testMethod4(String value) throws Exception {
        logger.info("In TestMethod4 : " + value);
        throw new NoSuchMethodException("그런 메소드 없습니다");
    }
}
