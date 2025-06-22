import boto3
import json
import os
import typing

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_ssm import SSMClient


endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)
dynamodb_resource = boto3.resource("dynamodb", endpoint_url=endpoint_url)

analyzer = SentimentIntensityAnalyzer()

def get_results_table_name() -> str:
    parameter = ssm.get_parameter(Name="/localstack-review-app/tables/results")
    return parameter["Parameter"]["Value"]

def classify_sentiment(text: str) -> dict:
    """
    Analyzes text using VADER and classifies sentiment.
    Returns the classification (positive, neutral, negative) and the compound score.
    """

    scores = analyzer.polarity_scores(text)
    compound_score = scores['compound']
    

    if compound_score >= 0.05:
        classification = "positive"
    elif compound_score <= -0.05:
        classification = "negative"
    else:
        classification = "neutral"
        
    return {"classification": classification, "score": compound_score}


def handler(event, context):
    """
    Lambda handler for sentiment analysis. Triggered by S3 object creation.
    """

    s3_event = event["Records"][0]["s3"]
    bucket_name = s3_event["bucket"]["name"]
    object_key = s3_event["object"]["key"]

    try:

        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        review_data = json.loads(response['Body'].read().decode('utf-8'))


        original_review = review_data["original_review"]
        review_text = original_review.get("reviewText", "")
        summary_text = original_review.get("summary", "")
        

        full_text = f"{summary_text}. {review_text}"
        
 
        sentiment_result = classify_sentiment(full_text)
        
    except Exception as e:
        print(f"Error processing file s3://{bucket_name}/{object_key}: {e}")
        return {"statusCode": 500, "body": json.dumps(f"Error: {str(e)}")}
        
    try:

        table_name = get_results_table_name()
        results_table = dynamodb_resource.Table(table_name)

        results_table.put_item(
            Item={
                'review_id': object_key,
                'reviewerID': original_review.get("reviewerID"),
                'sentiment': sentiment_result.get("classification"),
                'sentiment_score': str(sentiment_result.get("score", 0.0))

            }
        )
    except Exception as e:
        print(f"Error saving results to DynamoDB: {e}")
        return {"statusCode": 500, "body": json.dumps(f"DynamoDB Error: {str(e)}")}

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Sentiment analysis complete.",
            "review_id": object_key,
            "sentiment": sentiment_result
        })
    }