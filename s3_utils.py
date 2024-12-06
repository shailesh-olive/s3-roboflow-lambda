import os
import boto3
from botocore.config import Config
from dotenv import load_dotenv
load_dotenv()


FRAME_FOLDER = os.getenv('FRAME_FOLDER')
AWS_REGION = os.getenv('AWS_REGION')
IAM_ACCESS_ID = os.getenv('IAM_ACCESS_ID')
IAM_ACCESS_KEY = os.getenv('IAM_ACCESS_KEY')

s3 = boto3.client(
    's3', 
    aws_access_key_id=IAM_ACCESS_ID,
    aws_secret_access_key=IAM_ACCESS_KEY,
    region_name=AWS_REGION, 
    config=Config(signature_version='s3v4')
)


def generate_presigned_url(
    object_name: str, 
    bucket_name: str, 
) -> str:
    """Generate a presigned URL for an S3 object."""
    url = s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': object_name},
        ExpiresIn=3600
    )
    return url

def get_s3_objects(
    prefix: str,
    bucket_name: str
) -> list[str]:
    """
    Fetch the list of object keys in the given S3 bucket that match the prefix.
    """
    frames_prefix = f"{prefix}/{FRAME_FOLDER}"
    objects = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name, Prefix=frames_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                objects.append(obj['Key'])
    return objects