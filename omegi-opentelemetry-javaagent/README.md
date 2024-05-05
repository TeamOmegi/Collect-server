## 이슈 사항

python 서버에서 kafka로 읽을 때 decoding 해줘야 함
```python
    print(message.value.decode('utf-8'), flush=True)
```

## omegi-opentelemtety-javaagent 프로젝트 빌드

Gradle-Tasks-other-extendedAgent 실행

build/libs 경로에 omegi-opentelemtety-javaagent.jar 생성됨

## Class 설명

DemoAutoConfigurationCustomizerProvider : javaagent에서 customize 한 클래스를 사용하기 위한 명시 클래스

OmegiExtendedInstrumentation : Additional helper class로 메소드 enter/exit을 파악해 span 생성

OmegiExtendedInstrumentationModule : OmegiExtendedInstrumentation 등록 클래스

OmegiTraceSpanExporter : kafka로 trace를 전송하는 custom exporter

OmegiTraceSpanExporterFactory : OmegiTraceRecordExporter 등록 클래스

## META-INF/services

custom한 Exporter/Extension 명시

## data

```json
{
   "tracer":"omegi:omegi:1.0.0",
   "traceId":"c964f201ce7fa3d929e0a5160eadeaef",
   "error":{
      "exception.type":"java.lang.NoSuchMethodException",
      "exception.message":"그런 메소드 없습니다",
      "exception.flow":{
         "step.1":"org.omegi.test.YourService.testMethod4",
         "step.2":"org.omegi.test.MyService.testMethod3",
         "step.3":"org.omegi.test.MyService.testMethod2",
         "step.4":"org.omegi.test.MyController.test",
         "step.5":"java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0",
         "step.6":"java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke",
         "step.7":"java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke",
         "step.8":"java.base/java.lang.reflect.Method.invoke",
         "step.9":"org.springframework.web.method.support.InvocableHandlerMethod.doInvoke",
         "step.10":"org.springframework.web.method.support.InvocableHandlerMethod.invokeForRequest",
         "step.11":"org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod.invokeAndHandle",
         "step.12":"org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.invokeHandlerMethod",
         "step.13":"org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.handleInternal",
         "step.14":"org.springframework.web.servlet.mvc.method.AbstractHandlerMethodAdapter.handle",
         "step.15":"org.springframework.web.servlet.DispatcherServlet.doDispatch",
         "step.16":"org.springframework.web.servlet.DispatcherServlet.doService",
         "step.17":"org.springframework.web.servlet.FrameworkServlet.processRequest",
         "step.18":"org.springframework.web.servlet.FrameworkServlet.doGet",
         "step.19":"jakarta.servlet.http.HttpServlet.service",
         "step.20":"org.springframework.web.servlet.FrameworkServlet.service",
         "step.21":"jakarta.servlet.http.HttpServlet.service",
         "step.22":"org.apache.catalina.core.ApplicationFilterChain.internalDoFilter",
         "step.23":"org.apache.catalina.core.ApplicationFilterChain.doFilter",
         "step.24":"org.apache.tomcat.websocket.server.WsFilter.doFilter",
         "step.25":"org.apache.catalina.core.ApplicationFilterChain.internalDoFilter",
         "step.26":"org.apache.catalina.core.ApplicationFilterChain.doFilter",
         "step.27":"org.springframework.web.filter.RequestContextFilter.doFilterInternal",
         "step.28":"org.springframework.web.filter.OncePerRequestFilter.doFilter",
         "step.29":"org.apache.catalina.core.ApplicationFilterChain.internalDoFilter",
         "step.30":"org.apache.catalina.core.ApplicationFilterChain.doFilter",
         "step.31":"org.springframework.web.filter.FormContentFilter.doFilterInternal",
         "step.32":"org.springframework.web.filter.OncePerRequestFilter.doFilter",
         "step.33":"org.apache.catalina.core.ApplicationFilterChain.internalDoFilter",
         "step.34":"org.apache.catalina.core.ApplicationFilterChain.doFilter",
         "step.35":"org.springframework.web.servlet.v6_0.OpenTelemetryHandlerMappingFilter.doFilter",
         "step.36":"org.apache.catalina.core.ApplicationFilterChain.internalDoFilter",
         "step.37":"org.apache.catalina.core.ApplicationFilterChain.doFilter",
         "step.38":"org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal",
         "step.39":"org.springframework.web.filter.OncePerRequestFilter.doFilter",
         "step.40":"org.apache.catalina.core.ApplicationFilterChain.internalDoFilter",
         "step.41":"org.apache.catalina.core.ApplicationFilterChain.doFilter",
         "step.42":"org.apache.catalina.core.StandardWrapperValve.invoke",
         "step.43":"org.apache.catalina.core.StandardContextValve.invoke",
         "step.44":"org.apache.catalina.authenticator.AuthenticatorBase.invoke",
         "step.45":"org.apache.catalina.core.StandardHostValve.invoke",
         "step.46":"org.apache.catalina.valves.ErrorReportValve.invoke",
         "step.47":"org.apache.catalina.core.StandardEngineValve.invoke",
         "step.48":"org.apache.catalina.connector.CoyoteAdapter.service",
         "step.49":"org.apache.coyote.http11.Http11Processor.service",
         "step.50":"org.apache.coyote.AbstractProcessorLight.process",
         "step.51":"org.apache.coyote.AbstractProtocol$ConnectionHandler.process",
         "step.52":"org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun",
         "step.53":"org.apache.tomcat.util.net.SocketProcessorBase.run",
         "step.54":"org.apache.tomcat.util.threads.ThreadPoolExecutor.runWorker",
         "step.55":"org.apache.tomcat.util.threads.ThreadPoolExecutor$Worker.run",
         "step.56":"org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run",
         "step.57":"java.base/java.lang.Thread.run"
      },
      "exception.stacktrace":"java.lang.NoSuchMethodException: 그런 메소드 없습니다\n\tat org.omegi.test.YourService.testMethod4(YourService.java:14)\n\tat org.omegi.test.MyService.testMethod3(MyService.java:25)\n\tat org.omegi.test.MyService.testMethod2(MyService.java:20)\n\tat org.omegi.test.MyController.test(MyController.java:23)\n\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:78)\n\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n\tat java.base/java.lang.reflect.Method.invoke(Method.java:568)\n\tat org.springframework.web.method.support.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:255)\n\tat org.springframework.web.method.support.InvocableHandlerMethod.invokeForRequest(InvocableHandlerMethod.java:188)\n\tat org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod.invokeAndHandle(ServletInvocableHandlerMethod.java:118)\n\tat org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.invokeHandlerMethod(RequestMappingHandlerAdapter.java:926)\n\tat org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.handleInternal(RequestMappingHandlerAdapter.java:831)\n\tat org.springframework.web.servlet.mvc.method.AbstractHandlerMethodAdapter.handle(AbstractHandlerMethodAdapter.java:87)\n\tat org.springframework.web.servlet.DispatcherServlet.doDispatch(DispatcherServlet.java:1089)\n\tat org.springframework.web.servlet.DispatcherServlet.doService(DispatcherServlet.java:979)\n\tat org.springframework.web.servlet.FrameworkServlet.processRequest(FrameworkServlet.java:1014)\n\tat org.springframework.web.servlet.FrameworkServlet.doGet(FrameworkServlet.java:903)\n\tat jakarta.servlet.http.HttpServlet.service(HttpServlet.java:564)\n\tat org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:885)\n\tat jakarta.servlet.http.HttpServlet.service(HttpServlet.java:658)\n\tat org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:206)\n\tat org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:150)\n\tat org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:51)\n\tat org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:175)\n\tat org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:150)\n\tat org.springframework.web.filter.RequestContextFilter.doFilterInternal(RequestContextFilter.java:100)\n\tat org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116)\n\tat org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:175)\n\tat org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:150)\n\tat org.springframework.web.filter.FormContentFilter.doFilterInternal(FormContentFilter.java:93)\n\tat org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116)\n\tat org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:175)\n\tat org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:150)\n\tat org.springframework.web.servlet.v6_0.OpenTelemetryHandlerMappingFilter.doFilter(OpenTelemetryHandlerMappingFilter.java:69)\n\tat org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:175)\n\tat org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:150)\n\tat org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:201)\n\tat org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116)\n\tat org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:175)\n\tat org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:150)\n\tat org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:167)\n\tat org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:90)\n\tat org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:482)\n\tat org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:115)\n\tat org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:93)\n\tat org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:74)\n\tat org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:344)\n\tat org.apache.coyote.http11.Http11Processor.service(Http11Processor.java:391)\n\tat org.apache.coyote.AbstractProcessorLight.process(AbstractProcessorLight.java:63)\n\tat org.apache.coyote.AbstractProtocol$ConnectionHandler.process(AbstractProtocol.java:896)\n\tat org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1736)\n\tat org.apache.tomcat.util.net.SocketProcessorBase.run(SocketProcessorBase.java:52)\n\tat org.apache.tomcat.util.threads.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1191)\n\tat org.apache.tomcat.util.threads.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:659)\n\tat org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:63)\n\tat java.base/java.lang.Thread.run(Thread.java:831)\n"
   },
   "detailed-span1":{
      "name":"org.omegi.test.YourService.testMethod4",
      "spanId":"d558795ebba4801d",
      "parentSpanId":"96f31201f54028c2",
      "kind":"INTERNAL",
      "span enter-time":"2024-05-06 07:58:30.244",
      "span exit-time":"2024-05-06 07:58:30.251",
      "attributes":{
         "arguments":"[java.lang.String value : 파라미터 넘어갑니다]",
         "name":"org.omegi.test.YourService.testMethod4",
         "thread.name":"http-nio-8080-exec-1",
         "thread.id":28
      }
   },
   "detailed-span2":{
      "name":"org.omegi.test.MyService.testMethod2",
      "spanId":"96f31201f54028c2",
      "parentSpanId":"cb448bb04b333477",
      "kind":"INTERNAL",
      "span enter-time":"2024-05-06 07:58:30.242",
      "span exit-time":"2024-05-06 07:58:30.391",
      "attributes":{
         "arguments":"[]",
         "name":"org.omegi.test.MyService.testMethod2",
         "thread.name":"http-nio-8080-exec-1",
         "thread.id":28
      }
   },
   "detailed-span3":{
      "name":"GET /test",
      "spanId":"cb448bb04b333477",
      "parentSpanId":"0000000000000000",
      "kind":"SERVER",
      "span enter-time":"2024-05-06 07:58:29.818",
      "span exit-time":"2024-05-06 07:58:30.499",
      "attributes":{
         "url.scheme":"http",
         "thread.name":"http-nio-8080-exec-1",
         "server.port":8080,
         "user_agent.original":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
         "http.response.status_code":500,
         "network.protocol.version":"1.1",
         "thread.id":28,
         "url.path":"/test",
         "error.type":"500",
         "network.peer.port":60554,
         "server.address":"localhost",
         "client.address":"172.18.0.1",
         "network.peer.address":"172.18.0.1",
         "http.route":"/test",
         "http.request.method":"GET"
      }
   },
   "trace enter-time":"2024-05-06 07:58:29.818",
   "trace exit-time":"2024-05-06 07:58:30.499"
}
```

## 수정 계획

build.gradle 의존성 정리

데이터 중복 전송 수정

~~error 발생 로그만 kafka로 전송하도록 수정~~

~~trace 전송 형식 수정~~

