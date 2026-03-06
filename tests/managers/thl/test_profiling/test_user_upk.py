from datetime import datetime, timezone

from generalresearch.managers.thl.profiling.user_upk import UserUpkManager

now = datetime.now(tz=timezone.utc)
base = {
    "country_iso": "us",
    "language_iso": "eng",
    "timestamp": now,
}
upk_ans_dict = [
    {"pred": "gr:gender", "obj": "gr:male"},
    {"pred": "gr:age_in_years", "obj": "43"},
    {"pred": "gr:home_postal_code", "obj": "33143"},
    {"pred": "gr:ethnic_group", "obj": "gr:caucasians"},
    {"pred": "gr:ethnic_group", "obj": "gr:asian"},
]
for a in upk_ans_dict:
    a.update(base)


class TestUserUpkManager:

    def test_user_upk_empty(self, user_upk_manager: UserUpkManager, upk_data, user):
        res = user_upk_manager.get_user_upk_mysql(user_id=user.user_id)
        assert len(res) == 0

    def test_user_upk(self, user_upk_manager: UserUpkManager, upk_data, user):
        for x in upk_ans_dict:
            x["user_id"] = user.user_id
        user_upk = user_upk_manager.populate_user_upk_from_dict(upk_ans_dict)
        user_upk_manager.set_user_upk(upk_ans=user_upk)

        d = user_upk_manager.get_user_upk_simple(user_id=user.user_id)
        assert d["gender"] == "male"
        assert d["age_in_years"] == 43
        assert d["home_postal_code"] == "33143"
        assert d["ethnic_group"] == {"caucasians", "asian"}

        # Change my answers. age 43->44, gender male->female,
        #   ethnic->remove asian, add black_or_african_american
        for x in upk_ans_dict:
            if x["pred"] == "age_in_years":
                x["obj"] = "44"
            if x["pred"] == "gender":
                x["obj"] = "female"
        upk_ans_dict[-1]["obj"] = "black_or_african_american"
        user_upk = user_upk_manager.populate_user_upk_from_dict(upk_ans_dict)
        user_upk_manager.set_user_upk(upk_ans=user_upk)

        d = user_upk_manager.get_user_upk_simple(user_id=user.user_id)
        assert d["gender"] == "female"
        assert d["age_in_years"] == 44
        assert d["home_postal_code"] == "33143"
        assert d["ethnic_group"] == {"caucasians", "black_or_african_american"}

        age, gender = user_upk_manager.get_age_gender(user_id=user.user_id)
        assert age == 44
        assert gender == "female"
