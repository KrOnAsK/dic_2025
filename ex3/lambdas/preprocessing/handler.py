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

# used to make sure that S3 generates pre-signed URLs that have the localstack URL in them
endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)
dynamodb: "DynamoDBClient" = boto3.client("dynamodb", endpoint_url=endpoint_url)


def get_table_name() -> str:
    parameter = ssm.get_parameter(Name="/localstack-review-app/tables/reviews")
    return parameter["Parameter"]["Value"]


def handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    #bucket = get_bucket_name()

    table_name = get_table_name()

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            review_json = response["Body"].read().decode("utf-8")
            review_data = json.loads(review_json)
        except ClientError as e:
            return {
                "statusCode": 500,
                "body": f"Error retrieving object {key} from bucket {bucket}: {e.response['Error']['Message']}"
            }

    item = {
            "reviewerID": {"S": review_data["reviewerID"]},
            "asin": {"S": review_data["asin"]},
            "reviewerName": {"S": review_data.get("reviewerName", "")},
            "helpful": {"L": [{"N": str(n)} for n in review_data.get("helpful", [])]},
            "reviewText": {"S": review_data.get("reviewText", "")},
            "overall": {"N": str(review_data.get("overall", 0))},
            "summary": {"S": review_data.get("summary", "")},
            "unixReviewTime": {"N": str(review_data.get("unixReviewTime", 0))},
            "reviewTime": {"S": review_data.get("reviewTime", "")},
            "category": {"S": review_data.get("category", "")},
        }


    try:
        dynamodb.put_item(TableName=table_name, Item=item)
        print(f"Review {review_data['reviewerID']} saved to DynamoDB table {table_name}")
    except ClientError as e:
        return {
            "statusCode": 500,
            "body": f"Error saving review: {e.response['Error']['Message']}"
        }
    

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Review created successfully",
            "reviewId": review_data['reviewerID']
        })
    }



if __name__ == "__main__":
    print(handler(None, None))
