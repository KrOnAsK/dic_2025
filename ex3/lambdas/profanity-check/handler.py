import boto3
from better_profanity import profanity

import json
import os
import typing

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient

endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"
PROFANITY_RESULTS = os.environ.get("PROFANITY_RESULTS")

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)
dynamodb_resource = boto3.resource("dynamodb", endpoint_url=endpoint_url)


def get_customer_table_name() -> str:
    """Retrieves the customer table name from SSM Parameter Store."""
    parameter = ssm.get_parameter(Name=f"/localstack-review-app/tables/{PROFANITY_RESULTS}")
    return parameter["Parameter"]["Value"]


def handler(event, context):
    s3_event = event["Records"][0]["s3"]
    bucket_name = s3_event["bucket"]["name"]
    object_key = s3_event["object"]["key"]

    try:

        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        processed_review = json.loads(response['Body'].read().decode('utf-8'))

        summary_tokens = processed_review["processed"]["summary_tokens"]
        review_text_tokens = processed_review["processed"]["reviewText_tokens"]
        reviewer_id = processed_review["original_review"]["reviewerID"]

        is_profane = profanity.contains_profanity(summary_tokens) or profanity.contains_profanity(review_text_tokens)

    except Exception as e:
        print(f"Error processing file s3://{bucket_name}/{object_key}: {e}")
        return {"statusCode": 500, "body": json.dumps(f"Error: {str(e)}")}

    if is_profane:
        try:
            table_name = get_customer_table_name()
            customer_table = dynamodb_resource.Table(table_name)

            update_expression = "SET #uc = if_not_exists(#uc, :start) + :inc, #s = if_not_exists(#s, :status_ok)"

            response = customer_table.update_item(
                Key={'reviewerID': reviewer_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames={
                    '#uc': 'unpolite_count',
                    '#s': 'status'
                },
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':start': 0,
                    ':status_ok': 'ok'
                },
                ReturnValues="UPDATED_NEW"
            )

            new_unpolite_count = int(response['Attributes']['unpolite_count'])
            if new_unpolite_count > 3:
                customer_table.update_item(
                    Key={'reviewerID': reviewer_id},
                    UpdateExpression="SET #s = :status_banned",
                    ExpressionAttributeNames={'#s': 'status'},
                    ExpressionAttributeValues={':status_banned': 'banned'}
                )
                print(f"User {reviewer_id} has been banned.")

        except Exception as e:
            print(f"Error updating DynamoDB for user {reviewer_id}: {e}")
            return {"statusCode": 500, "body": json.dumps(f"DynamoDB Error: {str(e)}")}

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Profanity check complete.",
            "review_id": object_key,
            "is_profane": bool(is_profane)
        })
    }
