# Python 3.10 공식 이미지를 사용
FROM python:3.10

# 작업 디렉토리 설정
WORKDIR /app

# Python 의존성 파일 복사
COPY requirements.txt ./

# pip를 사용하여 requirements.txt에 명시된 패키지들을 설치
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 파일 복사
COPY . .

# 애플리케이션 실행
CMD ["python", "./main.py"]