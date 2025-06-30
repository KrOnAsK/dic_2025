from utils import upload_review, wait_ddb, PROFANITY_TABLE

def test_user_banned_after_four():
    user = "RUDE_BAN"
    for _ in range(4):
        upload_review("idiot badword", user)
    item = wait_ddb(PROFANITY_TABLE, "reviewerID", user, timeout=60)
    assert int(item["unpolite_count"]["N"]) >= 4
    assert item["status"]["S"] == "banned"