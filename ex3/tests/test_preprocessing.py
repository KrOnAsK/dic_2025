from utils import upload_review, wait_s3, PROCESSED_BUCKET

def test_preprocessing_tokens_present():
    key = upload_review("Brilliant product!", "U1", "Brilliant")
    processed = wait_s3(PROCESSED_BUCKET, key)
    toks = processed["processed"]
    assert toks["summary_tokens"], "summary tokens empty"
    assert toks["reviewText_tokens"], "reviewText tokens empty"