package org.omegi.test;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class MyController {
    private final MyService myService;
    private static final Logger LOGGER = LoggerFactory.getLogger(MyController.class);

    @GetMapping("/hello")
    public String hello() throws Exception {
        return "Hello, " + myMethod();
    }

    @GetMapping("/test")
    public String test() throws Exception {
        myService.testMethod2();
        return "test method";
    }

    @GetMapping("/test/{good}")
    public String test(@PathVariable String good) throws Exception {
        LOGGER.info("Hi this is test method : {}", good);
        myService.testMethod2();
        return "test method";
    }

    private String myMethod() throws Exception {
        myService.testMethod1();
        return "OpenTelemetry!";
    }
}