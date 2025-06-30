from utils import upload_review, wait_ddb, SENTIMENT_TABLE, PROFANITY_TABLE, PROCESSED_BUCKET, wait_s3

def test_full_chain_for_good_review():
    user = "CHAIN_USER"
    key = upload_review("Amazing! Will buy again.", user, "Awesome")
    
    #wait for preprocessing
    proc = wait_s3(PROCESSED_BUCKET, key)
    assert proc["processed"]["reviewText_tokens"]
    
    #wait for sentiment
    sent = wait_ddb(SENTIMENT_TABLE, "review_id", key)
    assert sent["sentiment"]["S"] == "positive"
    
    #check not banned
    prof = wait_ddb(PROFANITY_TABLE, "reviewerID", user)
    assert int(prof["unpolite_count"]["N"]) == 0
    assert prof["status"]["S"] == "ok"