from datetime import datetime, timezone

from generalresearch.models.morning.question import MorningQuestion

bid = {
    "buyer_account_id": "ab180f06-aa2b-4b8b-9b87-1031bfe8b16b",
    "buyer_id": "5f3b4daa-6ff0-4826-a551-9d4572ea1c84",
    "country_id": "us",
    "end_date": "2024-07-19T09:01:13.520243Z",
    "exclusions": [
        {"group_id": "66070689-5198-5782-b388-33daa74f3269", "lockout_period": 28}
    ],
    "id": "5324c2ac-eca8-4ed0-8b0e-042ba3aa2a85",
    "language_ids": ["en"],
    "name": "Ad-Hoc Survey",
    "published_at": "2024-06-19T09:01:13.520243Z",
    "quotas": [
        {
            "cost_per_interview": 154,
            "id": "b8ade883-a83d-4d8e-9ef7-953f4b692bd8",
            "qualifications": [
                {
                    "id": "age",
                    "response_ids": [
                        "18",
                        "19",
                        "20",
                        "21",
                        "22",
                        "23",
                        "24",
                        "25",
                        "26",
                        "27",
                        "28",
                        "29",
                        "30",
                        "31",
                        "32",
                        "33",
                        "34",
                    ],
                },
                {"id": "gender", "response_ids": ["1"]},
                {"id": "hispanic", "response_ids": ["1"]},
            ],
            "statistics": {
                "length_of_interview": 1353,
                "median_length_of_interview": 1353,
                "num_available": 3,
                "num_completes": 7,
                "num_failures": 0,
                "num_in_progress": 4,
                "num_over_quotas": 0,
                "num_qualified": 27,
                "num_quality_terminations": 14,
                "num_timeouts": 1,
                "qualified_conversion": 30,
            },
        }
    ],
    "state": "active",
    "statistics": {
        "earnings_per_click": 26,
        "estimated_length_of_interview": 1140,
        "incidence_rate": 77,
        "length_of_interview": 1198,
        "median_length_of_interview": 1198,
        "num_available": 70,
        "num_completes": 360,
        "num_entrants": 1467,
        "num_failures": 0,
        "num_in_progress": 48,
        "num_over_quotas": 10,
        "num_qualified": 1121,
        "num_quality_terminations": 584,
        "num_screenouts": 380,
        "num_timeouts": 85,
        "qualified_conversion": 34,
        "system_conversion": 25,
    },
    "supplier_exclusive": False,
    "survey_type": "ad_hoc",
    "timeout": 21600,
    "topic_id": "general",
}

bid = {
    "_experimental_single_use_qualifications": [
        {
            "id": "electric_car_test",
            "name": "Electric Car Test",
            "text": "What kind of vehicle do you drive?",
            "language_ids": ["en"],
            "responses": [{"id": "1", "text": "electric"}, {"id": "2", "text": "gas"}],
            "type": "multiple_choice",
        }
    ],
    "buyer_account_id": "0b6f207c-96e1-4dce-b032-566a815ad263",
    "buyer_id": "9020f6f3-db41-470a-a5d7-c04fa2da9156",
    "closed_at": "2022-01-01T00:00:00Z",
    "country_id": "us",
    "end_date": "2022-01-01T00:00:00Z",
    "exclusions": [
        {"group_id": "0bbae805-5a80-42e3-8d5f-cb056a0f825d", "lockout_period": 7}
    ],
    "id": "000f09a3-bc25-4adc-a443-a9975800e7ac",
    "language_ids": ["en", "es"],
    "name": "My Example Survey",
    "published_at": "2021-12-30T00:00:00Z",
    "quotas": [
        {
            "_experimental_single_use_qualifications": [
                {"id": "electric_car_test", "response_ids": ["1"]}
            ],
            "cost_per_interview": 100,
            "id": "6a7d0190-e6ad-4a59-9945-7ba460517f2b",
            "qualifications": [
                {"id": "gender", "response_ids": ["1"]},
                {"id": "age", "response_ids": ["18", "19", "20", "21"]},
            ],
            "statistics": {
                "length_of_interview": 600,
                "median_length_of_interview": 600,
                "num_available": 500,
                "num_completes": 100,
                "num_failures": 0,
                "num_in_progress": 0,
                "num_over_quotas": 0,
                "num_qualified": 100,
                "num_quality_terminations": 0,
                "num_timeouts": 0,
                "qualified_conversion": 100,
            },
        }
    ],
    "state": "active",
    "statistics": {
        "earnings_per_click": 50,
        "estimated_length_of_interview": 720,
        "incidence_rate": 100,
        "length_of_interview": 600,
        "median_length_of_interview": 600,
        "num_available": 500,
        "num_completes": 100,
        "num_entrants": 100,
        "num_failures": 0,
        "num_in_progress": 0,
        "num_over_quotas": 0,
        "num_qualified": 100,
        "num_quality_terminations": 0,
        "num_screenouts": 0,
        "num_timeouts": 0,
        "qualified_conversion": 100,
        "system_conversion": 100,
    },
    "supplier_exclusive": False,
    "survey_type": "ad_hoc",
    "timeout": 3600,
    "topic_id": "general",
}

# what gets run in MorningAPI._format_bid
bid["language_isos"] = ("eng",)
bid["country_iso"] = "us"
bid["end_date"] = datetime(2024, 7, 19, 9, 1, 13, 520243, tzinfo=timezone.utc)
bid["published_at"] = datetime(2024, 6, 19, 9, 1, 13, 520243, tzinfo=timezone.utc)
bid.update(bid["statistics"])
bid["qualified_conversion"] /= 100
bid["system_conversion"] /= 100
for quota in bid["quotas"]:
    quota.update(quota["statistics"])
    quota["qualified_conversion"] /= 100
    quota["cost_per_interview"] /= 100
if "_experimental_single_use_qualifications" in bid:
    bid["experimental_single_use_qualifications"] = [
        MorningQuestion.from_api(q, bid["country_iso"], "eng")
        for q in bid["_experimental_single_use_qualifications"]
    ]


class TestMorningBid:

    def test_model_validate(self):
        from generalresearch.models.morning.survey import MorningBid

        s = MorningBid.model_validate(bid)
        d = s.model_dump(mode="json")
        d = s.to_mysql()

    def test_manager(self):
        # todo: credentials n stuff
        pass
        # sql_helper = SqlHelper(host="localhost", user="root", password="", db="300large-morning")
        # m = MorningSurveyManager(sql_helper=sql_helper)
        # s = MorningBid.model_validate(bid)
        # m.create(s)
        # res = m.get_survey_library()[0]
        # MorningBid.model_validate(res)
