import boto3
from nltk.stem import WordNetLemmatizer
import nltk

import json
import os

endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"
STOPWORDS_BUCKET_NAME = os.environ.get("STOPWORDS_BUCKET")
STOPWORDS_KEY = os.environ.get("STOPWORDS_KEY", "stopwords.txt")
PROCESSED_BUCKET_NAME = os.environ.get("PROCESSED_BUCKET")

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)

# TODO: download locally and package into zip
nltk.data.path.append("/tmp")
nltk.download("punkt_tab", download_dir="/tmp")
nltk.download("wordnet", download_dir="/tmp")
lemmatizer = WordNetLemmatizer()


def get_bucket_name_stopwords() -> str:
    parameter = ssm.get_parameter(Name=f"/localstack-review-app/buckets/{STOPWORDS_BUCKET_NAME}")
    return parameter["Parameter"]["Value"]


def load_stopwords():
    """
    Loads stopwords from a specified file in S3.
    This makes the stopword list easily updatable without changing code.
    """
    reviews_bucket = get_bucket_name_stopwords()
    try:
        response = s3.get_object(Bucket=reviews_bucket, Key=STOPWORDS_KEY)
        stopwords_content = response['Body'].read().decode('utf-8')
        return set(stopwords_content.splitlines())
    except Exception as e:
        print(f"Error loading stopwords from s3://{reviews_bucket}/{STOPWORDS_KEY}: {e}")
        return set()


def preprocess_text(text: str, stopwords: set) -> list[str]:
    """
    Performs the full preprocessing pipeline on a given string of text.
    1. Lowercasing
    2. Tokenization
    3. Stop word removal
    4. Lemmatization
    """
    # 1. Lowercase the text
    text = text.lower()

    # 2. Tokenize the text
    tokens = nltk.word_tokenize(text)

    # 3. Filter out stopwords
    filtered_tokens = [token for token in tokens if token not in stopwords]

    # 4. Lemmatize the tokens
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]

    return lemmatized_tokens


def preprocess(event, context):
    """
    AWS Lambda handler function for preprocessing customer reviews.
    """
    stopwords = load_stopwords()

    s3_event = event["Records"][0]["s3"]
    review_bucket = s3_event["bucket"]["name"]
    review_key = s3_event["object"]["key"]

    try:
        response = s3.get_object(Bucket=review_bucket, Key=review_key)
        review_data = json.loads(response['Body'].read().decode('utf-8'))

        processed_summary = preprocess_text(review_data.get("summary", ""), stopwords)
        processed_review_text = preprocess_text(review_data.get("reviewText", ""), stopwords)

        output_data = {
            "original_review": review_data,
            "processed": {
                "summary_tokens": processed_summary,
                "reviewText_tokens": processed_review_text
            }
        }

        s3.put_object(
            Bucket=PROCESSED_BUCKET_NAME,
            Key=review_key,
            Body=json.dumps(output_data, indent=4),
            ContentType="application/json"
        )

        return {
            "statusCode": 200,
            "body": json.dumps(f"Successfully processed {review_key} and uploaded to {PROCESSED_BUCKET_NAME}.")
        }

    except Exception as e:
        print(f"Error processing file s3://{review_bucket}/{review_key}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing file: {str(e)}")
        }
