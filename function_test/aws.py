import os
from dotenv import load_dotenv


class AWS:
    def __init__(self):
        load_dotenv()
        self.access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.queue_url = os.getenv('SQS_QUEUE_URL')
        self.app_mode = os.getenv('APP_MODE')
        self.region = os.getenv('AWS_REGION')
        self.bucket = os.getenv('S3_BUCKET')