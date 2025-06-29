# Setup

Install [just](https://github.com/casey/just).

```bash
pip install -r requirements.txt
LAMBDA_RUNTIME_ENVIRONMENT_TIMEOUT=100 LAMBDA_LIMITS_CODE_SIZE_ZIPPED=524288000 DISABLE_CORS_HEADERS=1 DISABLE_CORS_CHECKS=1 DISABLE_CUSTOM_CORS_S3=1 DISABLE_CUSTOM_CORS_APIGATEWAY=1 LOCALSTACK_ACTIVATE_PRO=0 localstack start
just setup
```

## get urls

```bash
awslocal lambda get-function-url-config --function-name presign
awslocal lambda get-function-url-config --function-name list-reviews
awslocal lambda get-function-url-config --function-name list-preprocessing
awslocal lambda get-function-url-config --function-name db_list-profanity
awslocal lambda get-function-url-config --function-name db_list-sentiment
```

It should look something like `http://64sbkkdvit3h3fz7p97ad4paf2yvyc4w.lambda-url.us-east-1.localhost:4566/`.  
Note that you might have to delete the `localstack.cloud` at the end.

## example run

```bash
cat review_single.json | awslocal s3 cp - s3://reviews/$(uuidgen)

awslocal lambda invoke --function-name list-reviews output.json
awslocal lambda invoke --function-name list-preprocessing output.json
awslocal lambda invoke --function-name db_list-profanity output.json
awslocal lambda invoke --function-name db_list-sentiment output.json


awslocal s3 ls --summarize --human-readable --recursive s3://reviews
awslocal s3api get-object --bucket reviews --key f9ef1543-de58-49d4-87d5-d14f270c6bb2 output.json


awslocal dynamodb describe-table --table-name sentiment --query 'Table.ItemCount'
awslocal dynamodb describe-table --table-name profanity --query 'Table.ItemCount'

awslocal dynamodb execute-statement --statement "SELECT * FROM sentiment WHERE sentiment != 'neutral'"
awslocal dynamodb execute-statement --statement "SELECT * FROM profanity"
```

## full run

```bash
while IFS= read -r line; do
  echo "$line" | awslocal s3 cp - "s3://reviews/$(uuidgen)"
done < reviews_devset.json
```

```nushell
cat reviews_devset.json | lines | slice 1000..10000 | par-each -t 16 {|line| echo $line | awslocal s3 cp - s3://reviews/(random uuid) }
```