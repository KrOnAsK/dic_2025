from utils import upload_review, wait_ddb, SENTIMENT_TABLE, PROFANITY_TABLE, PROCESSED_BUCKET, wait_s3

def test_full_chain_for_bad_review():
    user = "CHAIN_USER"
    key = upload_review("Fuck Fuck Fucker.", user, "Awesome")
    
    #wait for preprocessing
    proc = wait_s3(PROCESSED_BUCKET, key)
    assert proc["processed"]["reviewText_tokens"]
    
    #wait for sentiment
    sent = wait_ddb(SENTIMENT_TABLE, "review_id", key)
    assert sent["sentiment"]["S"] == "negative"
    
    #check not banned
    prof = wait_ddb(PROFANITY_TABLE, "reviewerID", user)
    assert int(prof["unpolite_count"]["N"]) == 1
    assert prof["status"]["S"] == "ok"
