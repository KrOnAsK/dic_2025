import os
import typing

import boto3

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_ssm import SSMClient

# used to make sure that S3 generates pre-signed URLs that have the localstack URL in them
endpoint_url = None
if os.getenv("STAGE") == "local":
    endpoint_url = "https://localhost.localstack.cloud:4566"

s3: "S3Client" = boto3.client("s3", endpoint_url=endpoint_url)
ssm: "SSMClient" = boto3.client("ssm", endpoint_url=endpoint_url)


def get_bucket_name_reviews() -> str:
    parameter = ssm.get_parameter(Name="/localstack-review-app/buckets/reviews")
    return parameter["Parameter"]["Value"]


def handler(event, context):
    reviews_bucket = get_bucket_name_reviews()
    reviews = s3.list_objects(Bucket=reviews_bucket)

    if not reviews.get("Contents"):
        print(f"Bucket {reviews_bucket} is empty")
        return []

    result = {}
    # collect the original images
    for obj in reviews["Contents"]:
        result[obj["Key"]] = {
            "Name": obj["Key"],
            "Timestamp": obj["LastModified"].isoformat(),
            "Original": {
                "Size": obj["Size"],
                "URL": s3.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": reviews_bucket, "Key": obj["Key"]},
                    ExpiresIn=3600,
                ),
            },
        }

    return list(sorted(result.values(), key=lambda k: k["Timestamp"], reverse=True))


if __name__ == "__main__":
    print(handler(None, None))
