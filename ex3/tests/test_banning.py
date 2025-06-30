from utils import upload_review, wait_s3, wait_ddb, PROFANITY_TABLE, PROCESSED_BUCKET, SENTIMENT_TABLE
import time

def test_user_banned_after_four():
    user = "RUDE_BAN"
    # fire off 4 reviews, waiting for each one to be ingested & processed
    for _ in range(4):
        key = upload_review("fucking fucker fucking", user)
        wait_s3(PROCESSED_BUCKET, key)
        wait_ddb(SENTIMENT_TABLE, "review_id", key)

    # now poll the profanity table *until* we see the ban happen
    for _ in range(30):
        prof = wait_ddb(PROFANITY_TABLE, "reviewerID", user, timeout=1)
        if prof["status"]["S"] == "banned":
            break
        time.sleep(1)

    assert int(prof["unpolite_count"]["N"]) >= 4
    assert prof["status"]["S"] == "banned"