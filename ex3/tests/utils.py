import time, uuid, json, boto3, os

ENDPOINT = os.getenv("AWS_ENDPOINT", "http://localhost:4566")
RAW_BUCKET        = "reviews"
PROCESSED_BUCKET  = "reviews-processed"
PROFANITY_TABLE   = "profanity"
SENTIMENT_TABLE   = "sentiment"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

dyna = boto3.client(
    "dynamodb",
    endpoint_url=ENDPOINT,
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test"
)

def _uid() -> str:
    return uuid.uuid4().hex[:8]

def upload_review(text: str, reviewer: str, summary: str = "") -> str:
    key = f"{_uid()}.json"
    body = json.dumps(
        {"reviewerID": reviewer,
         "reviewText": text,
         "summary": summary,
         "overall": 3}
    )
    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=body)
    return key

#wait helpers

def wait_s3(bucket: str, key: str, timeout=30):
    for _ in range(timeout):
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read())
        except s3.exceptions.NoSuchKey:
            time.sleep(1)
    raise TimeoutError(f"s3://{bucket}/{key} not found")

def wait_ddb(table: str, key_attr: str, key_val: str, timeout=30):
    for _ in range(timeout):
        resp = dyna.get_item(TableName=table, Key={key_attr: {"S": key_val}})
        if "Item" in resp:
            return resp["Item"]
        time.sleep(1)
    raise TimeoutError(f"{table}:{key_val} not found")
