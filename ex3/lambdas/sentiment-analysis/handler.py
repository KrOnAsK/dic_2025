import boto3
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

import json
import os
import typing

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient

endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"
SENTIMENT_RESULTS = os.environ.get("SENTIMENT_RESULTS")

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)
dynamodb_resource = boto3.resource("dynamodb", endpoint_url=endpoint_url)

# TODO: download locally and package into zip
nltk.data.path.append("/tmp")
nltk.download("vader_lexicon", download_dir="/tmp")


def get_results_table_name() -> str:
    parameter = ssm.get_parameter(Name=f"/localstack-review-app/tables/{SENTIMENT_RESULTS}")
    return parameter["Parameter"]["Value"]


def classify_sentiment(text: str) -> dict:
    """
    Analyzes text using VADER and classifies sentiment.
    Returns the classification (positive, neutral, negative) and the compound score.
    """

    scores = SentimentIntensityAnalyzer().polarity_scores(text)
    neg_score = scores['neg']
    neu_score = scores['neu']
    pos_score = scores['pos']

    if pos_score > neu_score and pos_score > neg_score:
        classification = "positive"
    elif neg_score > neu_score and neg_score > pos_score:
        classification = "negative"
    else:
        classification = "neutral"

    return {
        "classification": classification,
        "score": scores["compound"],
        "score_negative": scores["neg"],
        "score_neutral": scores["neu"],
        "score_positive": scores["pos"],
    }


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

        review_text = review_data.get("reviewText", "")
        summary_text = review_data.get("summary", "")

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
                'reviewerID': review_data.get("reviewerID"),
                'sentiment': sentiment_result.get("classification"),
                'sentiment_score': str(sentiment_result.get("score", 0.0)),
                'sentiment_score_negative': str(sentiment_result.get("score_negative", 0.0)),
                'sentiment_score_neutral': str(sentiment_result.get("score_neutral", 0.0)),
                'sentiment_score_positive': str(sentiment_result.get("score_positive", 0.0))

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
