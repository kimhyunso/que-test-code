import json
import boto3
from dotenv import load_dotenv
import os
import time
from aws import AWS

aws = AWS()
sqs_client = boto3.client('sqs', region_name=aws.region)
s3_client = boto3.client('s3', region_name=aws.region)

DOWNLOAD_PATH = 'temp_downloads'
os.path.join(os.path.abspath(DOWNLOAD_PATH), 'test.mp3')

# --- (1) 실제 작업 함수들을 정의하거나 import ---
#     (이 함수들은 Consumer 코드베이스에 존재해야 함)
def process_video(video_path, s3key="dupilot-audio-bucket/audio.mp3", quality="medium", output_format="mp3"):
    print(f"--- Processing video ---")
    print(f"   Path: {video_path}")
    print(f"   Quality: {quality}")
    print(f"   Format: {output_format}")
    print(f'  s3key: {s3key}')
    path = os.path.join(os.path.abspath(DOWNLOAD_PATH), 'test.mp3')

    s3_client.download_file(video_path, s3key, path)
    time.sleep(3) # 작업 시뮬레이션
    print(f"--- Video processing complete ---")
    return {"status": "success", "output_path": f"processed_{os.path.basename(video_path)}"}

def send_notification(user_id, message):
    print(f"--- Sending notification ---")
    print(f"   To: {user_id}")
    print(f"   Message: {message}")
    time.sleep(1) # 작업 시뮬레이션
    print(f"--- Notification sent ---")
    return {"status": "sent"}

# --- (2) 실행할 함수 이름과 실제 함수 객체를 매핑 ---
#     (보안상 중요: SQS 메시지만 보고 임의 코드를 실행하지 않도록 함)
AVAILABLE_TASKS = {
    "process_video": process_video,
    "send_notification": send_notification,
    # 여기에 Consumer가 실행할 수 있는 모든 함수를 등록
}

sqs_client = boto3.client('sqs', region_name=aws.region)

print("✅ Consumer(Worker)가 시작되었습니다. SQS 대기 중...")

while True:
    receipt_handle = None # finally에서 참조하기 위해 초기화
    try:
        response = sqs_client.receive_message(
            QueueUrl=aws.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            AttributeNames=['All'] # FIFO 큐의 Group ID 등을 위해
        )

        if "Messages" not in response:
            continue

        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        # 메시지 본문 파싱
        task_info = json.loads(message['Body'])
        function_name = task_info.get("function_name")
        args = task_info.get("args", [])
        kwargs = task_info.get("kwargs", {})

        # 등록된 함수인지 확인하고 실행
        if function_name in AVAILABLE_TASKS:
            target_function = AVAILABLE_TASKS[function_name]
            print(f"\n🚀 Received task: Calling {function_name}(args={args}, kwargs={kwargs})")
            
            # --- (4) 함수 실행 ---
            result = target_function(*args, **kwargs) # *와 **로 인자 언패킹
            
            print(f"✅ Task completed. Result: {result}")

            # 성공 시 메시지 삭제
            sqs_client.delete_message(QueueUrl=aws.queue_url, ReceiptHandle=receipt_handle)
            receipt_handle = None # 삭제 성공 시 핸들 초기화
        
        else:
            print(f"❗️ Error: Unknown function name '{function_name}'. Skipping.")
            # 알 수 없는 함수는 그냥 삭제 (또는 DLQ로 이동)
            sqs_client.delete_message(QueueUrl=aws.queue_url, ReceiptHandle=receipt_handle)
            receipt_handle = None # 삭제 성공 시 핸들 초기화

    except Exception as e:
        print(f"❗️ Error processing message: {e}")
        # 오류 발생 시 메시지를 삭제하지 않음 (재시도 또는 DLQ)
        time.sleep(5) # 잠시 대기 후 재시도
        
    # finally:
        # 실패 시에도 receipt_handle이 남아있으면 여기서 처리할 로직 (예: DLQ 보내기)
        # pass