from utils import upload_review, wait_ddb, PROFANITY_TABLE

def test_unpolite_counter_increments():
    user = "RUDE_Y"
    key = upload_review("you fucking idiot", user)
    _ = wait_ddb(PROFANITY_TABLE, "reviewerID", user)
    item = wait_ddb(PROFANITY_TABLE, "reviewerID", user)
    assert int(item["unpolite_count"]["N"]) == 1
    assert item["status"]["S"] == "ok"