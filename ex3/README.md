# Setup

```sh
pip install -r requirements.txt
DISABLE_CORS_HEADERS=1 DISABLE_CORS_CHECKS=1 DISABLE_CUSTOM_CORS_S3=1 DISABLE_CUSTOM_CORS_APIGATEWAY=1 LOCALSTACK_ACTIVATE_PRO=0 LOCALSTACK_DEBUG=1 localstack start
just setup
```

## get urls

```sh
awslocal lambda get-function-url-config --function-name presign
awslocal lambda get-function-url-config --function-name list-reviews
awslocal lambda get-function-url-config --function-name list-preprocessing
awslocal lambda get-function-url-config --function-name db_list-profanity
awslocal lambda get-function-url-config --function-name db_list-sentiment
```

it should look something like `http://64sbkkdvit3h3fz7p97ad4paf2yvyc4w.lambda-url.us-east-1.localhost:4566/`

## example run

```sh
awslocal s3api put-object --bucket reviews --key (random uuid) --body review_single.json

awslocal lambda invoke --function-name list-reviews output.json
awslocal lambda invoke --function-name list-preprocessing output.json
awslocal lambda invoke --function-name db_list-profanity output.json
awslocal lambda invoke --function-name db_list-sentiment output.json
```