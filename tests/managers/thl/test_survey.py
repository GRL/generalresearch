import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from generalresearch.models import Source
from generalresearch.models.legacy.bucket import (
    SurveyEligibilityCriterion,
    TopNPlusBucket,
    DurationSummary,
    PayoutSummary,
)
from generalresearch.models.thl.profiling.user_question_answer import (
    UserQuestionAnswer,
)
from generalresearch.models.thl.survey.model import (
    Survey,
    SurveyStat,
    SurveyCategoryModel,
    SurveyEligibilityDefinition,
)


@pytest.fixture(scope="session")
def surveys_fixture():
    return [
        Survey(source=Source.TESTING, survey_id="a", buyer_code="buyer1"),
        Survey(source=Source.TESTING, survey_id="b", buyer_code="buyer2"),
        Survey(source=Source.TESTING, survey_id="c", buyer_code="buyer2"),
    ]


ssa = SurveyStat(
    survey_source=Source.TESTING,
    survey_survey_id="a",
    survey_is_live=True,
    quota_id="__all__",
    cpi=Decimal(1),
    country_iso="us",
    version=1,
    complete_too_fast_cutoff=300,
    prescreen_conv_alpha=10,
    prescreen_conv_beta=10,
    conv_alpha=10,
    conv_beta=10,
    dropoff_alpha=10,
    dropoff_beta=10,
    completion_time_mu=1,
    completion_time_sigma=0.4,
    mobile_eligible_alpha=10,
    mobile_eligible_beta=10,
    desktop_eligible_alpha=10,
    desktop_eligible_beta=10,
    tablet_eligible_alpha=10,
    tablet_eligible_beta=10,
    long_fail_rate=1,
    user_report_coeff=1,
    recon_likelihood=0,
    score_x0=0,
    score_x1=1,
    score=100,
)
ssb = ssa.model_dump()
ssb["survey_source"] = Source.TESTING
ssb["survey_survey_id"] = "b"
ssb["completion_time_mu"] = 2
ssb["score"] = 90
ssb = SurveyStat.model_validate(ssb)


class TestSurvey:

    def test(
        self,
        delete_buyers_surveys,
        buyer_manager,
        survey_manager,
        surveys_fixture,
    ):
        survey_manager.create_or_update(surveys_fixture)
        survey_ids = {s.survey_id for s in surveys_fixture}
        res = survey_manager.filter_by_natural_key(
            source=Source.TESTING, survey_ids=survey_ids
        )
        assert len(res) == len(surveys_fixture)
        assert res[0].id is not None
        surveys2 = surveys_fixture.copy()
        surveys2.append(
            Survey(source=Source.TESTING, survey_id="d", buyer_code="buyer2")
        )
        survey_manager.create_or_update(surveys2)
        survey_ids = {s.survey_id for s in surveys2}
        res2 = survey_manager.filter_by_natural_key(
            source=Source.TESTING, survey_ids=survey_ids
        )
        assert res2[0].id == res[0].id
        assert res2[0] == res[0]
        assert len(res2) == len(surveys2)

    def test_category(self, survey_manager):
        survey1 = Survey(id=562289, survey_id="a", source=Source.TESTING)
        survey2 = Survey(id=562290, survey_id="a", source=Source.TESTING)
        categories = list(survey_manager.category_manager.categories.values())
        sc = [SurveyCategoryModel(category=c, strength=1 / 2) for c in categories[:2]]
        sc2 = [SurveyCategoryModel(category=c, strength=1 / 2) for c in categories[2:4]]
        survey1.categories = sc
        survey2.categories = sc2
        surveys = [survey1, survey2]
        survey_manager.update_surveys_categories(surveys)

    def test_survey_eligibility(
        self, survey_manager, upk_data, question_manager, uqa_manager
    ):
        bucket = TopNPlusBucket(
            id="c82cf98c578a43218334544ab376b00e",
            contents=[],
            duration=DurationSummary(max=1, min=1, q1=1, q2=1, q3=1),
            quality_score=1,
            payout=PayoutSummary(max=1, min=1, q1=1, q2=1, q3=1),
            uri="https://task.generalresearch.com/api/v1/52d3f63b2709/00ff1d9b71b94bf4b20d22cd56774120/?i=2a4a897a76464af2b85703b72a125da0&b=379fb74f-05b2-42dc-b283-47e1c8678b04&66482fb=82fe142",
        )

        survey1 = Survey(
            survey_id="a",
            source=Source.TESTING,
            eligibility_criteria=SurveyEligibilityDefinition(
                # "5d6d9f3c03bb40bf9d0a24f306387d7c",  # gr:gender
                # "c1309f099ab84a39b01200a56dac65cf",  # d:600
                # "90e86550ddf84b08a9f7f5372dd9651b",  # i:gender
                # "1a8216ddb09440a8bc748cf8ca89ecec",  # i:adhoc_13126
                property_codes=("i:adhoc_13126", "i:gender", "i:something"),
            ),
        )
        # might have a couple surveys in the bucket ... merge them all together
        qualifying_questions = set(survey1.eligibility_criteria.property_codes)

        uqas = [
            UserQuestionAnswer(
                question_id="5d6d9f3c03bb40bf9d0a24f306387d7c",
                answer=("1",),
                calc_answers={"i:gender": ("1",), "d:1": ("2",)},
                country_iso="us",
                language_iso="eng",
                property_code="gr:gender",
            ),
            UserQuestionAnswer(
                question_id="c1309f099ab84a39b01200a56dac65cf",
                answer=("50796", "50784"),
                country_iso="us",
                language_iso="eng",
                property_code="d:600",
                calc_answers={"d:600": ("50796", "50784")},
            ),
            UserQuestionAnswer(
                question_id="1a8216ddb09440a8bc748cf8ca89ecec",
                answer=("3", "4"),
                country_iso="us",
                language_iso="eng",
                property_code="i:adhoc_13126",
                calc_answers={"i:adhoc_13126": ("3", "4")},
            ),
        ]
        uqad = dict()
        for uqa in uqas:
            for k, v in uqa.calc_answers.items():
                if k in qualifying_questions:
                    uqad[k] = uqa
                    uqad[uqa.property_code] = uqa

        question_ids = {uqa.question_id for uqa in uqad.values()}
        qs = question_manager.get_multi_upk(question_ids)
        # Sort question by LOWEST task_count (rarest)
        qs = sorted(qs, key=lambda x: x.importance.task_count if x.importance else 0)
        # qd = {q.id: q for q in qs}

        q = [x for x in qs if x.ext_question_id == "i:adhoc_13126"][0]
        q.explanation_template = "You have been diagnosed with: {answer}."
        q = [x for x in qs if x.ext_question_id == "gr:gender"][0]
        q.explanation_template = "Your gender is {answer}."

        ecs = []
        for q in qs:
            answer_code = uqad[q.ext_question_id].answer
            answer_label = tuple([q.choices_text_lookup[ans] for ans in answer_code])
            explanation = None
            if q.explanation_template:
                explanation = q.explanation_template.format(
                    answer=", ".join(answer_label)
                )
            sec = SurveyEligibilityCriterion(
                question_id=q.id,
                question_text=q.text,
                qualifying_answer=answer_code,
                qualifying_answer_label=answer_label,
                explanation=explanation,
                property_code=q.ext_question_id,
            )
            print(sec)
            ecs.append(sec)
        bucket.eligibility_criteria = tuple(ecs)
        print(bucket)


class TestSurveyStat:
    def test(
        self,
        delete_buyers_surveys,
        surveystat_manager,
        survey_manager,
        surveys_fixture,
    ):
        survey_manager.create_or_update(surveys_fixture)
        ss = [ssa, ssb]
        surveystat_manager.update_or_create(ss)
        keys = [s.unique_key for s in ss]
        res = surveystat_manager.filter_by_unique_keys(keys)
        assert len(res) == 2
        assert res[0].conv_alpha == 10

        ssa.conv_alpha = 11
        surveystat_manager.update_or_create([ssa])
        res = surveystat_manager.filter_by_unique_keys([ssa.unique_key])
        assert len(res) == 1
        assert res[0].conv_alpha == 11

    @pytest.mark.skip()
    def test_big(
        self,
        # delete_buyers_surveys,
        surveystat_manager,
        survey_manager,
        surveys_fixture,
    ):
        survey = surveys_fixture[0].model_copy()
        surveys = []
        for idx in range(20_000):
            s = survey.model_copy()
            s.survey_id = uuid.uuid4().hex
            surveys.append(s)

        survey_manager.create_or_update(surveys)
        # Realistically, we're going to have like, say 20k surveys
        #   and 99% of them will be updated each time
        survey_stats = []
        for survey in surveys:
            ss = ssa.model_copy()
            ss.survey__survey_id = survey.survey_id
            ss.quota_id = "__all__"
            ss.score = 10
            survey_stats.append(ss)
        print(len(survey_stats))
        print(survey_stats[12].natural_key, survey_stats[2000].natural_key)
        print(f"----a-----: {datetime.now().isoformat()}")
        res = surveystat_manager.update_or_create(survey_stats)
        print(f"----b-----: {datetime.now().isoformat()}")
        assert len(res) == 20_000
        return

        # 1,000 of the 20,000 are "new"
        now = datetime.now(tz=timezone.utc)
        for s in ss[:1000]:
            s.survey__survey_id = "b"
            s.updated_at = now
        # 18,000 need to be updated (scores update or whatever)
        for s in ss[1000:-1000]:
            s.score = 20
            s.conv_alpha = 20
            s.conv_beta = 20
            s.updated_at = now
        # and 1,000 don't change
        print(f"----c-----: {datetime.now().isoformat()}")
        res2 = surveystat_manager.update_or_create(ss)
        print(f"----d-----: {datetime.now().isoformat()}")
        assert len(res2) == 20_000

    def test_ymsp(
        self,
        delete_buyers_surveys,
        surveys_fixture,
        survey_manager,
        surveystat_manager,
    ):
        source = Source.TESTING
        survey = surveys_fixture[0].model_copy()
        surveys = []
        for idx in range(100):
            s = survey.model_copy()
            s.survey_id = uuid.uuid4().hex
            surveys.append(s)
        survey_stats = []
        for survey in surveys:
            ss = ssa.model_copy()
            ss.survey_survey_id = survey.survey_id
            survey_stats.append(ss)

        surveystat_manager.update_surveystats_for_source(
            source=source, surveys=surveys, survey_stats=survey_stats
        )
        # UPDATE -------
        since = datetime.now(tz=timezone.utc)
        print(f"{since=}")

        # 10 survey disappear
        surveys = surveys[10:]

        # and 2 new ones are created
        for idx in range(2):
            s = survey.model_copy()
            s.survey_id = uuid.uuid4().hex
            surveys.append(s)
        survey_stats = []
        for survey in surveys:
            ss = ssa.model_copy()
            ss.survey_survey_id = survey.survey_id
            survey_stats.append(ss)
        surveystat_manager.update_surveystats_for_source(
            source=source, surveys=surveys, survey_stats=survey_stats
        )

        live_surveys = survey_manager.filter_by_source_live(source=source)
        assert len(live_surveys) == 92  # 100 - 10 + 2

        ss = surveystat_manager.filter_by_updated_since(since=since)
        assert len(ss) == 102  # 92 existing + 10 not live

        ss = surveystat_manager.filter_by_live()
        assert len(ss) == 92

    def test_filter(
        self,
        delete_buyers_surveys,
        surveys_fixture,
        survey_manager,
        surveystat_manager,
    ):
        surveys = []
        survey = surveys_fixture[0].model_copy()
        survey.source = Source.TESTING
        survey.survey_id = "a"
        surveys.append(survey.model_copy())
        survey.source = Source.TESTING
        survey.survey_id = "b"
        surveys.append(survey.model_copy())
        survey.source = Source.TESTING2
        survey.survey_id = "b"
        surveys.append(survey.model_copy())
        survey.source = Source.TESTING2
        survey.survey_id = "c"
        surveys.append(survey.model_copy())
        # 4 surveys t:a, t:b, u:b, u:c

        survey_stats = []
        for survey in surveys:
            ss = ssa.model_copy()
            ss.survey_survey_id = survey.survey_id
            ss.survey_source = survey.source
            survey_stats.append(ss)

        surveystat_manager.update_surveystats_for_source(
            source=Source.TESTING, surveys=surveys[:2], survey_stats=survey_stats[:2]
        )
        surveystat_manager.update_surveystats_for_source(
            source=Source.TESTING2, surveys=surveys[2:], survey_stats=survey_stats[2:]
        )

        survey_keys = [f"{s.source.value}:{s.survey_id}" for s in surveys]
        res = surveystat_manager.filter(survey_keys=survey_keys, min_score=0.01)
        assert len(res) == 4
        res = survey_manager.filter_by_keys(survey_keys)
        assert len(res) == 4

        res = surveystat_manager.filter(survey_keys=survey_keys[:2])
        assert len(res) == 2
        res = survey_manager.filter_by_keys(survey_keys[:2])
        assert len(res) == 2
