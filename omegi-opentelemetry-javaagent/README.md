### omegi-opentelemtety-javaagent 프로젝트 빌드
Gradle-Tasks-other-extendedAgent 실행
build/libs 경로에 omegi-opentelemtety-javaagent.jar 생성됨

### Class 설명
DemoAutoConfigurationCustomizerProvider : javaagent에서 customize 한 클래스를 사용하기 위한 명시 클래스
OmegiExtendedInstrumentation : Additional helper class로 메소드 enter/exit을 파악해 span 생성
OmegiExtendedInstrumentationModule : OmegiExtendedInstrumentation 등록 클래스
OmegiLogRecordExporter : log를 전송하는 custom exporter(추후 없애거나 커스텀 log 찍는 용으로 변경할 예정)
OmegiLogRecordExporterFactory : OmegiLogRecordExporter 등록 클래스
OmegiTraceRecordExporter : kafka로 trace를 전송하는 custom exporter
OmegiTraceRecordExporterFactory : OmegiTraceRecordExporter 등록 클래스

### META-INF/services
custom한 Exporter/instrumentation 명시

### 수정 계획
build.gradle 의존성 정리
error 발생 로그만 kafka로 전송하도록 수정
trace 전송 형식 수정
