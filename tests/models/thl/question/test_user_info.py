from generalresearch.models.thl.profiling.user_info import UserInfo


class TestUserInfo:

    def test_init(self):

        s = (
            '{"user_profile_knowledge": [], "marketplace_profile_knowledge": [{"source": "d", "question_id": '
            '"1", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "pr", '
            '"question_id": "3", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": '
            '"h", "question_id": "60", "answer": ["58"], "created": "2023-11-07T16:41:05.234096Z"}, '
            '{"source": "c", "question_id": "43", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, '
            '{"source": "s", "question_id": "211", "answer": ["111"], "created": '
            '"2023-11-07T16:41:05.234096Z"}, {"source": "s", "question_id": "1843", "answer": ["111"], '
            '"created": "2023-11-07T16:41:05.234096Z"}, {"source": "h", "question_id": "13959", "answer": ['
            '"244155"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "c", "question_id": "33092", '
            '"answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "c", "question_id": "gender", '
            '"answer": ["10682"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "e", "question_id": '
            '"gender", "answer": ["male"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "f", '
            '"question_id": "gender", "answer": ["male"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": '
            '"i", "question_id": "gender", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, '
            '{"source": "c", "question_id": "137510", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, '
            '{"source": "m", "question_id": "gender", "answer": ["1"], "created": '
            '"2023-11-07T16:41:05.234096Z"}, {"source": "o", "question_id": "gender", "answer": ["male"], '
            '"created": "2023-11-07T16:41:05.234096Z"}, {"source": "c", "question_id": "gender_plus", "answer": ['
            '"7657644"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "i", "question_id": '
            '"gender_plus", "answer": ["1"], "created": "2023-11-07T16:41:05.234096Z"}, {"source": "c", '
            '"question_id": "income_level", "answer": ["9071"], "created": "2023-11-07T16:41:05.234096Z"}]}'
        )
        instance = UserInfo.model_validate_json(s)
        assert isinstance(instance, UserInfo)
