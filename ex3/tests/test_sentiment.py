from utils import upload_review, wait_ddb, SENTIMENT_TABLE

def _sentiment_for(key):
    item = wait_ddb(SENTIMENT_TABLE, "review_id", key)
    return item["sentiment"]["S"]

def test_positive_negative():
    key_pos = upload_review("I absolutely love this!", "POS1")
    assert _sentiment_for(key_pos) == "positive"

    key_neg = upload_review("This is terrible and awful.", "NEG1")
    assert _sentiment_for(key_neg) == "negative"