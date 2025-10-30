import os
import json
import boto3
from dotenv import load_dotenv
from aws import AWS

load_dotenv()
aws = AWS()
file_name = 'audio2.mp3'

s3_client = boto3.client('s3', region_name=aws.region)
sqs_client = boto3.client('sqs', region_name=aws.region)

s3_key = f"uploads/{file_name}" # S3 내의 저장 경로 (예: "uploads/my-audio.mp3")
            
s3_client.upload_file(
    Filename=file_name, 
    Bucket=aws.bucket, 
    Key=s3_key
)
print("S3 Upload successful.")

# 4. SQS에 "참조" 메시지 전송
message_body = json.dumps({
    "s3Bucket": aws.bucket,
    "s3Key": s3_key
})

print(f"Sending message to SQS: {message_body}")
sqs_client.send_message(
    QueueUrl=aws.queue_url,
    MessageBody=message_body,
    MessageGroupId=file_name, 
    MessageDeduplicationId=file_name
)
print("SQS message sent.")








