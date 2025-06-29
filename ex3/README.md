# Setup

Install [just](https://github.com/casey/just).

```sh
pip install -r requirements.txt
LAMBDA_LIMITS_CODE_SIZE_ZIPPED=524288000 DISABLE_CORS_HEADERS=1 DISABLE_CORS_CHECKS=1 DISABLE_CUSTOM_CORS_S3=1 DISABLE_CUSTOM_CORS_APIGATEWAY=1 LOCALSTACK_ACTIVATE_PRO=0 LOCALSTACK_DEBUG=1 localstack start
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

It should look something like `http://64sbkkdvit3h3fz7p97ad4paf2yvyc4w.lambda-url.us-east-1.localhost:4566/`.  
Note that you might have to delete the `localstack.cloud` at the end.

## example run

```sh
cat review_single.json | awslocal s3 cp - s3://reviews/$(uuidgen)

awslocal lambda invoke --function-name list-reviews output.json
awslocal lambda invoke --function-name list-preprocessing output.json
awslocal lambda invoke --function-name db_list-profanity output.json
awslocal lambda invoke --function-name db_list-sentiment output.json
```

## full run

```sh
while IFS= read -r line; do
  echo "$line" | awslocal s3 cp - "s3://reviews/$(uuidgen)"
done < reviews_devset.json
```