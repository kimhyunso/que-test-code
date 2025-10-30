# 선행작업
1. AWS 엑세스키, 비밀키 설정하기 -> iam
2. .env 파일을 만들어 아래 내용처럼 `value` 채우기


### env 파일
```env
AWS_ACCESS_KEY_ID="value"
AWS_SECRET_ACCESS_KEY="value"

SQS_QUEUE_URL="value"
APP_MODE="value"
AWS_REGION="value"
S3_BUCKET="value"
```


# producer = backend 서버
1. s3에 원본 비디오, 오디오를 저장한다.
2. aws sqs에 해당 비디오, 오디오 url을 json형식으로 보낸다.

# consumer = Worker
1. sqs에서 받은 데이터를 토대로 s3에서 비디오, 오디오 파일을 저장한다.
2. [STT->MT->LLM->TTS] 과정을 진행한다.

## 고려사항
sqs에 json형식의 데이터를 저장할 때, 함수와 매개변수를 저장하여 보낼 수 있음
또한 s3에 uploads/file_name.mp3처럼 `/` 한단계로 나누어있지 않고, 트리처럼 여러단계에 걸처 나누어져 있다면 consumer에서 58번라인을 수정해야함





