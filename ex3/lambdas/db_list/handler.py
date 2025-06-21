import json
import os
import typing

import boto3
from botocore.exceptions import ClientError
from datetime import datetime

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient
    from mypy_boto3_dynamodb import DynamoDBClient

endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"

ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)
dynamodb: "DynamoDBClient" = boto3.client("dynamodb", endpoint_url=endpoint_url)

def get_table_name() -> str:
    parameter = ssm.get_parameter(Name="/localstack-review-app/tables/reviews")
    return parameter["Parameter"]["Value"]

def handler(event, context):
    try:
        table = dynamodb.Table(get_table_name())
        response = table.scan()
        items = response.get('Items', [])
        return {
            'statusCode': 200,
            'body': json.dumps(items)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

if __name__ == "__main__":
    print(handler(None, None))