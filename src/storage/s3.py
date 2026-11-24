import boto3

from src.config import settings

# 모듈 전역으로 한 번만 생성해 Lambda warm start 시 재사용
s3_client = boto3.client("s3", region_name=settings.AWS_REGION)
