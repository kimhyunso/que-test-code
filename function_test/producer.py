import json
import boto3
from dotenv import load_dotenv
import os
from aws import AWS


aws = AWS()
load_dotenv()

sqs_client = boto3.client('sqs', region_name=aws.region)

def call_remote_function(function_name, *args, **kwargs):
    """
    실행할 함수 이름과 인자들을 SQS 메시지로 보냅니다.
    """
    message_body = json.dumps({
        "function_name": function_name,
        "args": args,         # 위치 기반 인자 (tuple)
        "kwargs": kwargs      # 키워드 기반 인자 (dict)
    })

    try:
        print(f"Sending task to SQS: Call {function_name} with args={args}, kwargs={kwargs}")
        response = sqs_client.send_message(
            QueueUrl=aws.queue_url,
            MessageBody=message_body,
            MessageGroupId=function_name,
            MessageDeduplicationId=function_name
            # (FIFO 큐라면 MessageGroupId, MessageDeduplicationId 추가)
        )
        print(f"Task sent. Message ID: {response.get('MessageId')}")
        return response.get('MessageId')
    except Exception as e:
        print(f"Send Task Failed: {e}")
        return None

# --- 예시: 원격 함수 호출 ---
if __name__ == "__main__":
    # 1. process_video 함수 호출 요청 (위치 + 키워드 인자)
    call_remote_function(
        "process_video",               # 함수 이름
        "s3://dupilot-audio-bucket/audio.mp3", # args[0]
        s3key="dupilot-audio-bucket/audio.mp3",
        quality="high",                # kwargs['quality']
        output_format="mp3"            # kwargs['output_format']
    )

    # 2. send_notification 함수 호출 요청 (키워드 인자만)
    call_remote_function(
        "send_notification",           # 함수 이름
        user_id="user123",             # kwargs['user_id']
        message="Your video is ready!" # kwargs['message']
    )