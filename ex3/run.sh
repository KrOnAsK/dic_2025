#!/bin/bash
echo "Activating virtual environment..."
source .venv/bin/activate

#echo "Emptying reviews bucket..."
#awslocal s3 rm s3://localstack-review-app-reviews --recursive
#awslocal dynamodb delete-table --table-name reviews

create_bucket_if_not_exists() {
  BUCKET_NAME=$1
  if ! awslocal s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
    echo "Creating bucket $BUCKET_NAME..."
    awslocal s3 mb s3://"$BUCKET_NAME"
  else
    echo "Bucket $BUCKET_NAME already exists, skipping creation."
  fi
}

create_table_if_not_exists() {
  local TABLE_NAME=$1

  if awslocal dynamodb describe-table --table-name "$TABLE_NAME" >/dev/null 2>&1; then
    echo "DynamoDB table $TABLE_NAME already exists, skipping creation."
  else
    echo "Creating DynamoDB table $TABLE_NAME..."
    awslocal dynamodb create-table \
      --table-name "$TABLE_NAME" \
      --attribute-definitions AttributeName=review_id,AttributeType=S \
      --key-schema AttributeName=review_id,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST
  fi

  echo "Adding/updating SSM parameter for DynamoDB table $TABLE_NAME..."
  awslocal ssm put-parameter --name /localstack-review-app/tables/reviews \
    --type String --value "$TABLE_NAME" --overwrite
}

echo "Creating S3 buckets and DynamoDB tables..."
create_bucket_if_not_exists localstack-review-app-reviews
create_table_if_not_exists reviews


echo "Creating SSM parameters..."
awslocal ssm put-parameter --name /localstack-review-app/buckets/reviews \
  --type String --value "localstack-review-app-reviews" --overwrite

echo "Creating pre-signed URL Lambda function..."
(cd lambdas/presign; rm -f lambda.zip; zip lambda.zip handler.py)

awslocal lambda delete-function --function-name presign || true

awslocal lambda create-function \
  --function-name presign \
  --runtime python3.11 \
  --timeout 10 \
  --zip-file fileb://lambdas/presign/lambda.zip \
  --handler handler.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment Variables="{STAGE=local}"

awslocal lambda create-function-url-config \
  --function-name presign \
  --auth-type NONE

echo "Creating preprocessing Lambda function..."
(cd lambdas/preprocessing; rm -f lambda.zip; zip lambda.zip handler.py)

awslocal lambda delete-function --function-name preprocessing || true

awslocal lambda create-function \
  --function-name preprocessing \
  --runtime python3.11 \
  --timeout 10 \
  --zip-file fileb://lambdas/preprocessing/lambda.zip \
  --handler handler.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment Variables="{STAGE=local}"

awslocal lambda create-function-url-config \
  --function-name preprocessing \
  --auth-type NONE

echo "Creating list Lambda function..."
(cd lambdas/list; rm -f lambda.zip; zip lambda.zip handler.py)

awslocal lambda delete-function --function-name list || true

awslocal lambda create-function \
  --function-name list \
  --runtime python3.11 \
  --timeout 10 \
  --zip-file fileb://lambdas/list/lambda.zip \
  --handler handler.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment Variables="{STAGE=local}"

awslocal lambda create-function-url-config \
  --function-name list \
  --auth-type NONE

cho "Creating db_list Lambda function..."
(cd lambdas/db_list; rm -f lambda.zip; zip lambda.zip handler.py)

awslocal lambda delete-function --function-name db_list || true

awslocal lambda create-function \
  --function-name db_list \
  --runtime python3.11 \
  --timeout 10 \
  --zip-file fileb://lambdas/db_list/lambda.zip \
  --handler handler.handler \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --environment Variables="{STAGE=local}"

awslocal lambda create-function-url-config \
  --function-name db_list \
  --auth-type NONE

echo "Creating trigger(notification) for preprocessing Lambda function..."
awslocal s3api put-bucket-notification-configuration \
  --bucket localstack-review-app-reviews \
  --notification-configuration "{\"LambdaFunctionConfigurations\":\
  [{\"LambdaFunctionArn\": \"$(awslocal lambda get-function --function-name preprocessing | jq -r .Configuration.FunctionArn)\",\
  \"Events\": [\"s3:ObjectCreated:*\"]}]}"

echo "Creating webapp S3 bucket and website..."
create_bucket_if_not_exists review-webapp

awslocal s3 sync ./website s3://review-webapp

awslocal s3 website s3://review-webapp --index-document index.html

echo "Open the web application in your browser..."
echo "http://review-webapp.s3-website.localhost.localstack.cloud:4566"
#open http://review-webapp.s3-website.localhost.localstack.cloud:4566
