import boto3
from nltk.stem import WordNetLemmatizer
import nltk

import json
import re
import os

nltk.data.path.append("/tmp")
nltk.download("wordnet", download_dir="/tmp")
nltk.download("omw-1.4", download_dir="/tmp")

s3_client = boto3.client("s3")
lemmatizer = WordNetLemmatizer()

WORD_RE = re.compile(
    r"[\s\t\d\(\)\[\]\{\}\.\!\?\,\;\:\+\=\-\_\"\'`\~\#\@\&\*\%\€\$\§\\\/]+"
)


def load_stopwords_from_s3(bucket, key):
    """
    Loads stopwords from a specified file in S3.
    This makes the stopword list easily updatable without changing code.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        stopwords_content = response['Body'].read().decode('utf-8')
        return set(stopwords_content.splitlines())
    except Exception as e:
        print(f"Error loading stopwords from s3://{bucket}/{key}: {e}")
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

    # 2. Tokenize the text and filter out short tokens
    tokens = [token for token in WORD_RE.split(text) if len(token) > 1]

    # 3. Filter out stopwords
    filtered_tokens = [token for token in tokens if token not in stopwords]

    # 4. Lemmatize the tokens
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]

    return lemmatized_tokens


def preprocess(event, context):
    """
    AWS Lambda handler function for preprocessing customer reviews.
    """

    source_bucket = os.environ.get("SOURCE_BUCKET")
    destination_bucket = os.environ.get("DESTINATION_BUCKET")
    stopwords_key = os.environ.get("STOPWORDS_KEY", "stopwords.txt")

    if not all([source_bucket, destination_bucket]):
        return {
            "statusCode": 500,
            "body": json.dumps("Error: SOURCE_BUCKET or DESTINATION_BUCKET environment variables not set.")
        }

    stopwords = load_stopwords_from_s3(source_bucket, stopwords_key)

    s3_event = event["Records"][0]["s3"]
    review_bucket = s3_event["bucket"]["name"]
    review_key = s3_event["object"]["key"]

    try:

        response = s3_client.get_object(Bucket=review_bucket, Key=review_key)
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

        s3_client.put_object(
            Bucket=destination_bucket,
            Key=review_key,
            Body=json.dumps(output_data, indent=4),
            ContentType="application/json"
        )

        return {
            "statusCode": 200,
            "body": json.dumps(f"Successfully processed {review_key} and uploaded to {destination_bucket}.")
        }

    except Exception as e:
        print(f"Error processing file s3://{review_bucket}/{review_key}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error processing file: {str(e)}")
        }
