import json
import os
import typing

import boto3

if typing.TYPE_CHECKING:
    from mypy_boto3_ssm import SSMClient
    from mypy_boto3_dynamodb import DynamoDBServiceResource

endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"

ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)
dynamodb: "DynamoDBServiceResource" = boto3.resource("dynamodb", endpoint_url=endpoint_url)


def get_table_name() -> str:
    parameter = ssm.get_parameter(Name="/localstack-review-app/tables/reviews")
    return parameter["Parameter"]["Value"]


def handler(event, context):
    try:
        table = dynamodb.Table(get_table_name())
        response = table.scan()
        print(f"Scanned table {get_table_name()} successfully.")
        print(f"Response: {response}")
        items = response.get('Items', [])
        print(f"Retrieved {len(items)} items from the table.")
        return {
            'statusCode': 200,
            'body': json.dumps(items)
        }
    except Exception as e:
        print(f"Error scanning table: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


if __name__ == "__main__":
    print(handler(None, None))
