import json
import os
import typing
import uuid

import boto3
from botocore.exceptions import ClientError

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient

# used to make sure that S3 generates pre-signed URLs that have the localstack URL in them
endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"
REVIEWS_BUCKET_NAME = os.environ.get("REVIEWS_BUCKET")

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)


def get_bucket_name() -> str:
    parameter = ssm.get_parameter(Name=f"/localstack-review-app/buckets/{REVIEWS_BUCKET_NAME}")
    return parameter["Parameter"]["Value"]


def handler(event, context):
    # make sure the bucket exists
    bucket = get_bucket_name()
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)

    # generate the pre-signed POST url
    key = str(uuid.uuid4())
    url = s3.generate_presigned_post(
        Bucket=bucket,
        Key=key,
        Fields={"Content-Type": "application/json"},
        Conditions=[{"Content-Type": "application/json"}]
    )

    # return it!
    return {"statusCode": 200, "body": json.dumps(url)}


if __name__ == "__main__":
    print(handler(None, None))
