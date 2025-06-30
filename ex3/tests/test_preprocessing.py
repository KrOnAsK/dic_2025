import pytest
from utils import upload_review, wait_s3, PROCESSED_BUCKET

def get_processed_tokens(review_text, summary):
    key = upload_review(review_text, "U_test", summary)
    processed = wait_s3(PROCESSED_BUCKET, key)
    return processed["processed"]

def test_preprocessing_tokens_present():
    key = upload_review("Brilliant product!", "U1", "Brilliant")
    processed = wait_s3(PROCESSED_BUCKET, key)
    toks = processed["processed"]
    assert toks["summary_tokens"], "summary tokens empty"
    assert toks["reviewText_tokens"], "reviewText tokens empty"

def test_preprocessing_stemming_summary():
    # Example: if stemming is working, "Running" should yield a token with "run"
    tokens = get_processed_tokens("Some review text", "Running")["summary_tokens"]
    stemmed = any("run" in token.lower() for token in tokens)
    assert stemmed, "Expected stemming in summary tokens not found"

def test_preprocessing_stemming_reviewText():
    # Example: if stemming is working, "swimming" should yield a token with "swim"
    tokens = get_processed_tokens("I love swimming in the ocean", "Review")["reviewText_tokens"]
    stemmed = any("swim" in token.lower() for token in tokens)
    assert stemmed, "Expected stemming in reviewText tokens not found"

def test_preprocessing_no_unexpected_tokens():
    # Check that tokens do not include isolated punctuation symbols
    tokens = get_processed_tokens("Amazing really amazing...", "Cool Review")["reviewText_tokens"]
    unexpected = [tok for tok in tokens if tok in ("!", ".", ",", ":", ";")]
    assert not unexpected, f"Unexpected punctuation tokens found: {unexpected}"