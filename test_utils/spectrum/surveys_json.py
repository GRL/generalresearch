from generalresearch.models import LogicalOperator
from generalresearch.models.spectrum.survey import (
    SpectrumCondition,
    SpectrumSurvey,
)
from generalresearch.models.thl.survey.condition import ConditionValueType

SURVEYS_JSON = [
    '{"cpi":"3.90","country_isos":["us"],"language_isos":["eng"],"buyer_id":"215","bid_loi":780,"source":"s",'
    '"used_question_ids":["1235","212"],"survey_id":"111111","survey_name":"Exciting New Survey #14472374",'
    '"status":22,"field_end_date":"2023-03-02T07:05:36.261000Z","category_code":"232","calculation_type":"COMPLETES",'
    '"requires_pii":false,"survey_exclusions":"13947261,14126487,14361592,14376811,14385771,14387789,14472374",'
    '"exclusion_period":30,"bid_ir":0.2,"overall_loi":null,"overall_ir":null,"last_block_loi":null,'
    '"last_block_ir":null,"project_last_complete_date":null,"country_iso":"us","language_iso":"eng",'
    '"include_psids":null,"exclude_psids":null'
    ',"qualifications":["ee5e842","e6e0b0b"],"quotas":[{"remaining_count":100,'
    '"condition_hashes":["32cbf31"]}],"conditions":null,"created_api":"2023-02-28T07:05:36.698000Z",'
    '"modified_api":"2024-03-10T09:43:40.030000Z","updated":"2024-05-30T21:52:46.431612Z","is_live":true'
    "}",
    '{"cpi":"3.90","country_isos":["us"],"language_isos":["eng"],"buyer_id":"215","bid_loi":780,"source":"s",'
    '"used_question_ids":["1235","212"],"survey_id":"14472374","survey_name":"Exciting New Survey #14472374",'
    '"status":22,"field_end_date":"2023-03-02T07:05:36.261000Z","category_code":"232","calculation_type":"COMPLETES",'
    '"requires_pii":false,"survey_exclusions":"13947261,14126487,14361592,14376811,14385771,14387789,14472374",'
    '"exclusion_period":30,"bid_ir":0.2,"overall_loi":null,"overall_ir":null,"last_block_loi":null,'
    '"last_block_ir":null,"project_last_complete_date":null,"country_iso":"us","language_iso":"eng",'
    '"include_psids":null,"exclude_psids":"0408319875e9dbffdc09e86671ad5636,23c4c66ecbc465906d0b0fd798740e64,'
    '861df4603df3b7f754b8d4b89cbdb313","qualifications":["ee5e842","e6e0b0b"],"quotas":[{"remaining_count":100,'
    '"condition_hashes":["32cbf31"]}],"conditions":null,"created_api":"2023-02-28T07:05:36.698000Z",'
    '"modified_api":"2024-03-10T09:43:40.030000Z","updated":"2024-05-30T21:52:46.431612Z","is_live":true'
    "}",
    '{"cpi":"3.90","country_isos":["us"],"language_isos":["eng"],"buyer_id":"215","bid_loi":780,"source":"s",'
    '"used_question_ids":["1235","212"],"survey_id":"12345","survey_name":"Exciting New Survey #14472374",'
    '"status":22,"field_end_date":"2023-03-02T07:05:36.261000Z","category_code":"232","calculation_type":"COMPLETES",'
    '"requires_pii":false,"survey_exclusions":"13947261,14126487,14361592,14376811,14385771,14387789,14472374",'
    '"exclusion_period":30,"bid_ir":0.2,"overall_loi":null,"overall_ir":null,"last_block_loi":null,'
    '"last_block_ir":null,"project_last_complete_date":null,"country_iso":"us","language_iso":"eng",'
    '"include_psids":"7d043991b1494dbbb57786b11c88239c","exclude_psids":null'
    ',"qualifications":["ee5e842","e6e0b0b"],"quotas":[{"remaining_count":100,'
    '"condition_hashes":["32cbf31"]}],"conditions":null,"created_api":"2023-02-28T07:05:36.698000Z",'
    '"modified_api":"2024-03-10T09:43:40.030000Z","updated":"2024-05-30T21:52:46.431612Z","is_live":true'
    "}",
    '{"cpi":"1.40","country_isos":["us"],"language_isos":["eng"],"buyer_id":"233","bid_loi":null,"source":"s",'
    '"used_question_ids":["245","244","212","211","225"],"survey_id":"14970164","survey_name":"Exciting New Survey '
    '#14970164","status":22,"field_end_date":"2024-05-07T16:18:33.000000Z","category_code":"232",'
    '"calculation_type":"COMPLETES","requires_pii":false,"survey_exclusions":"14970164,29690277",'
    '"exclusion_period":30,"bid_ir":null,"overall_loi":900,"overall_ir":0.56,"last_block_loi":600,'
    '"last_block_ir":0.01,"project_last_complete_date":"2024-05-28T04:12:56.297000Z","country_iso":"us",'
    '"language_iso":"eng","include_psids":null,"exclude_psids":"01c7156fd9639737effbbdebd7fd66f6,'
    "0508b88f4991bac8b10e9de74ce80194,0a51c627d77cef41f802e51a00126697,15b888176ac4781c2c978a9a05c396f8,"
    "17bc146b4f7fb05c7058d25da70c6a44,29935289c1f86a4144aab2e12652f305,2fe9d1d451efca10eba4fa4e5e2b74c9,"
    "c3527b7ef570a1571ea19870f3c25600,cdf2771d57cda9f1bf334382b2b7afd8,cebf3ec50395d973310ea526457dd5a0,"
    "cf3877cfc15e2e6ef2a56a7a7a37f3d3,dfa691e6d060e3643d5731df30be9f69,e0cb49537182660826aa351e1187809f,"
    'edb6d280113ca49561f25fdcb500fde6,fbfba66cfad602f1c26e61e6174eb1f7,fd4307b16fd15e8534a4551c9b6872fc",'
    '"qualifications":["1ab337d","a01aa68","437774f","dc6065b","82b6ad6"],"quotas":[{"remaining_count":242,'
    '"condition_hashes":["c23c0b9"]},{"remaining_count":0,"condition_hashes":["5b8c6cf"]},{"remaining_count":126,'
    '"condition_hashes":["ac35a6e"]},{"remaining_count":110,"condition_hashes":["5e7e5aa"]},{"remaining_count":108,'
    '"condition_hashes":["9a7aef3"]},{"remaining_count":127,"condition_hashes":["4f75127"]},{"remaining_count":0,'
    '"condition_hashes":["95437ed"]},{"remaining_count":17,"condition_hashes":["b4b7b95"]},{"remaining_count":16,'
    '"condition_hashes":["0ab0ae6"]},{"remaining_count":8,"condition_hashes":["6e86fb5"]},{"remaining_count":12,'
    '"condition_hashes":["24de31e"]},{"remaining_count":69,"condition_hashes":["6bdf350"]},{"remaining_count":411,'
    '"condition_hashes":["c94d422"]}],"conditions":null,"created_api":"2023-03-30T22:47:36.324000Z",'
    '"modified_api":"2024-05-30T13:07:16.489000Z","updated":"2024-05-30T21:52:37.493282Z","is_live":true,'
    '"all_hashes":["c94d422","b4b7b95","6bdf350","6e86fb5","82b6ad6","24de31e","1ab337d","c23c0b9","9a7aef3",'
    '"ac35a6e","95437ed","5b8c6cf","437774f","a01aa68","5e7e5aa","4f75127","0ab0ae6","dc6065b"]}',
    '{"cpi":"1.23","country_isos":["au"],"language_isos":["eng"],"buyer_id":"215","bid_loi":780,"source":"s",'
    '"used_question_ids":[],"survey_id":"69420","survey_name":"Everyone is eligible AU",'
    '"status":22,"field_end_date":"2023-03-02T07:05:36.261000Z","category_code":"232","calculation_type":"COMPLETES",'
    '"requires_pii":false,"survey_exclusions":"13947261,14126487,14361592,14376811,14385771,14387789,14472374",'
    '"exclusion_period":30,"bid_ir":0.2,"overall_loi":null,"overall_ir":null,"last_block_loi":null,'
    '"last_block_ir":null,"project_last_complete_date":null,"country_iso":"au","language_iso":"eng",'
    '"include_psids":null,"exclude_psids":null'
    ',"qualifications":[],"quotas":[{"remaining_count":100,'
    '"condition_hashes":[]}],"conditions":null,"created_api":"2023-02-28T07:05:36.698000Z",'
    '"modified_api":"2024-03-10T09:43:40.030000Z","updated":"2024-05-30T21:52:46.431612Z","is_live":true'
    "}",
    '{"cpi":"1.23","country_isos":["us"],"language_isos":["eng"],"buyer_id":"215","bid_loi":780,"source":"s",'
    '"used_question_ids":[],"survey_id":"69421","survey_name":"Everyone is eligible US",'
    '"status":22,"field_end_date":"2023-03-02T07:05:36.261000Z","category_code":"232","calculation_type":"COMPLETES",'
    '"requires_pii":false,"survey_exclusions":"13947261,14126487,14361592,14376811,14385771,14387789,14472374",'
    '"exclusion_period":30,"bid_ir":0.2,"overall_loi":null,"overall_ir":null,"last_block_loi":null,'
    '"last_block_ir":null,"project_last_complete_date":null,"country_iso":"us","language_iso":"eng",'
    '"include_psids":null,"exclude_psids":null'
    ',"qualifications":[],"quotas":[{"remaining_count":100,'
    '"condition_hashes":[]}],"conditions":null,"created_api":"2023-02-28T07:05:36.698000Z",'
    '"modified_api":"2024-03-10T09:43:40.030000Z","updated":"2024-05-30T21:52:46.431612Z","is_live":true'
    "}",
    # For partial eligibility
    '{"cpi":"1.23","country_isos":["us"],"language_isos":["eng"],"buyer_id":"215","bid_loi":780,"source":"s",'
    '"used_question_ids":["1031", "212"],"survey_id":"999000","survey_name":"Pet owners",'
    '"status":22,"field_end_date":"2023-03-02T07:05:36.261000Z","category_code":"232","calculation_type":"COMPLETES",'
    '"requires_pii":false,"survey_exclusions":"13947261",'
    '"exclusion_period":30,"bid_ir":0.2,"overall_loi":null,"overall_ir":null,"last_block_loi":null,'
    '"last_block_ir":null,"project_last_complete_date":null,"country_iso":"us","language_iso":"eng",'
    '"include_psids":null,"exclude_psids":null'
    ',"qualifications":["0039b0c", "00f60a8"],"quotas":[{"remaining_count":100,'
    '"condition_hashes":[]}],"conditions":null,"created_api":"2023-02-28T07:05:36.698000Z",'
    '"modified_api":"2024-03-10T09:43:40.030000Z","updated":"2024-05-30T21:52:46.431612Z","is_live":true'
    "}",
]

# make sure hashes for 111111 are in db
c1 = SpectrumCondition(
    question_id="1001",
    value_type=ConditionValueType.LIST,
    values=["a", "b", "c"],
    negate=False,
    logical_operator=LogicalOperator.OR,
)
c2 = SpectrumCondition(
    question_id="1001",
    value_type=ConditionValueType.LIST,
    values=["a"],
    negate=False,
    logical_operator=LogicalOperator.OR,
)
c3 = SpectrumCondition(
    question_id="1002",
    value_type=ConditionValueType.RANGE,
    values=["18-24", "30-32"],
    negate=False,
    logical_operator=LogicalOperator.OR,
)
c4 = SpectrumCondition(
    question_id="212",
    value_type=ConditionValueType.LIST,
    values=["23", "24"],
    negate=False,
    logical_operator=LogicalOperator.OR,
)
c5 = SpectrumCondition(
    question_id="1031",
    value_type=ConditionValueType.LIST,
    values=["113", "114", "121"],
    negate=False,
    logical_operator=LogicalOperator.OR,
)
CONDITIONS = [c1, c2, c3, c4, c5]
survey = SpectrumSurvey.model_validate_json(SURVEYS_JSON[0])
assert c1.criterion_hash in survey.qualifications
assert c3.criterion_hash in survey.qualifications
