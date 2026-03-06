# from decimal import Decimal
#
# from datetime import timezone, datetime
# from pymysql import IntegrityError
# from generalresearch.models.precision.survey import PrecisionSurvey
# from tests.models.precision import survey_json


# def delete_survey(survey_id: str):
#     db_name = sql_helper.db
#     # TODO: what is the precision specific db name...
#
#     sql_helper.execute_sql_query(
#         query="""
#         DELETE FROM `300large-precision`.precision_survey
#         WHERE survey_id = %s
#         """,
#         params=[survey_id], commit=True)
#     sql_helper.execute_sql_query("""
#     DELETE FROM `300large-precision`.precision_survey_country WHERE survey_id = %s
#     """, [survey_id], commit=True)
#     sql_helper.execute_sql_query("""
#     DELETE FROM `300large-precision`.precision_survey_language WHERE survey_id = %s
#     """, [survey_id], commit=True)
#
#
# class TestPrecisionSurvey:
#     def test_survey_create(self):
#         now = datetime.now(tz=timezone.utc)
#         s = PrecisionSurvey.model_validate(survey_json)
#         self.assertEqual(s.survey_id, '0000')
#         delete_survey(s.survey_id)
#
#         sm.create(s)
#
#         surveys = sm.get_survey_library(updated_since=now)
#         self.assertEqual(len(surveys), 1)
#         self.assertEqual('0000', surveys[0].survey_id)
#         self.assertTrue(s.is_unchanged(surveys[0]))
#
#         with self.assertRaises(IntegrityError) as context:
#             sm.create(s)
#
#     def test_survey_update(self):
#         # There's extra complexity here with the country/lang join tables
#         now = datetime.now(tz=timezone.utc)
#         s = PrecisionSurvey.model_validate(survey_json)
#         self.assertEqual(s.survey_id, '0000')
#         delete_survey(s.survey_id)
#         sm.create(s)
#         s.cpi = Decimal('0.50')
#         # started out at only 'ca' and 'eng'
#         s.country_isos = ['us']
#         s.country_iso = 'us'
#         s.language_isos = ['eng', 'spa']
#         s.language_iso = 'eng'
#         sm.update([s])
#         surveys = sm.get_survey_library(updated_since=now)
#         self.assertEqual(len(surveys), 1)
#         s2 = surveys[0]
#         self.assertEqual('0000', s2.survey_id)
#         self.assertEqual(Decimal('0.50'), s2.cpi)
#         self.assertTrue(s.is_unchanged(s2))
