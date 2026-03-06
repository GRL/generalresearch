class TestDynataCondition:

    def test_condition_create(self):
        from generalresearch.models.dynata.survey import DynataCondition

        cell = {
            "tag": "90606986-5508-461b-a821-216e9a72f1a0",
            "attribute_id": 120,
            "negate": False,
            "kind": "VALUE",
            "value": "45398",
        }
        c = DynataCondition.from_api(cell)
        assert c.evaluate_criterion({"120": {"45398"}})
        assert not c.evaluate_criterion({"120": {"11111"}})

        cell = {
            "tag": "aa7169c0-cb34-499a-aadd-31e0013df8fd",
            "attribute_id": 231302,
            "negate": False,
            "operator": "OR",
            "kind": "LIST",
            "list": ["514802", "514804", "514808", "514810"],
        }
        c = DynataCondition.from_api(cell)
        assert c.evaluate_criterion({"231302": {"514804", "123445"}})
        assert not c.evaluate_criterion({"231302": {"123445"}})

        cell = {
            "tag": "aa7169c0-cb34-499a-aadd-31e0013df8fd",
            "attribute_id": 231302,
            "negate": False,
            "operator": "AND",
            "kind": "LIST",
            "list": ["514802", "514804"],
        }
        c = DynataCondition.from_api(cell)
        assert c.evaluate_criterion({"231302": {"514802", "514804"}})
        assert not c.evaluate_criterion({"231302": {"514802"}})

        cell = {
            "tag": "75a36c67-0328-4c1b-a4dd-67d34688ff68",
            "attribute_id": 80,
            "negate": False,
            "kind": "RANGE",
            "range": {"from": 18, "to": 99},
        }
        c = DynataCondition.from_api(cell)
        assert c.evaluate_criterion({"80": {"20"}})
        assert not c.evaluate_criterion({"80": {"120"}})

        cell = {
            "tag": "dd64b622-ed10-4a3b-e1h8-a4e63b59vha2",
            "attribute_id": 83,
            "negate": False,
            "kind": "INEFFABLE",
        }
        c = DynataCondition.from_api(cell)
        assert c.evaluate_criterion({"83": {"20"}})

        cell = {
            "tag": "kei35kkjj-d00k-52kj-b3j4-a4jinx9832",
            "attribute_id": 8,
            "negate": False,
            "kind": "ANSWERED",
        }
        c = DynataCondition.from_api(cell)
        assert c.evaluate_criterion({"8": {"20"}})
        assert not c.evaluate_criterion({"81": {"20"}})

    def test_condition_range(self):
        from generalresearch.models.dynata.survey import DynataCondition

        cell = {
            "tag": "75a36c67-0328-4c1b-a4dd-67d34688ff68",
            "attribute_id": 80,
            "negate": False,
            "kind": "RANGE",
            "range": {"from": 18, "to": None},
        }
        c = DynataCondition.from_api(cell)
        assert c.evaluate_criterion({"80": {"20"}})

    def test_recontact(self):
        from generalresearch.models.dynata.survey import DynataCondition

        cell = {
            "tag": "d559212d-7984-4239-89c2-06c29588d79e",
            "attribute_id": 238384,
            "negate": False,
            "operator": "OR",
            "kind": "INVITE_COLLECTIONS",
            "invite_collections": ["621041", "621042"],
        }
        c = DynataCondition.from_api(cell)
        assert c.evaluate_criterion({"80": {"20"}}, user_groups={"621041", "a"})


class TestDynataSurvey:
    pass

    # def test_survey_eligibility(self):
    #     d = {'survey_id': 29333264, 'survey_name': '#29333264', 'survey_status': 22,
    #          'field_end_date': datetime(2024, 5, 23, 18, 18, 31, tzinfo=timezone.utc),
    #          'category': 'Exciting New', 'category_code': 232,
    #          'crtd_on': datetime(2024, 5, 20, 17, 48, 13, tzinfo=timezone.utc),
    #          'mod_on': datetime(2024, 5, 20, 18, 18, 31, tzinfo=timezone.utc),
    #          'soft_launch': False, 'click_balancing': 0, 'price_type': 1, 'pii': False,
    #          'buyer_message': '', 'buyer_id': 4726, 'incl_excl': 0,
    #          'cpi': Decimal('1.20000'), 'last_complete_date': None, 'project_last_complete_date': None,
    #          'quotas': [], 'qualifications': [],
    #          'country_iso': 'fr', 'language_iso': 'fre', 'overall_ir': 0.4, 'overall_loi': 600,
    #          'last_block_ir': None, 'last_block_loi': None, 'survey_exclusions': set(), 'exclusion_period': 0}
    #     s = DynataSurvey.from_api(d)
    #     s.qualifications = ['a', 'b', 'c']
    #     s.quotas = [
    #         SpectrumQuota(remaining_count=10, condition_hashes=['a', 'b']),
    #         SpectrumQuota(remaining_count=0, condition_hashes=['d']),
    #         SpectrumQuota(remaining_count=10, condition_hashes=['e'])
    #     ]
    #
    #     self.assertTrue(s.passes_qualifications({'a': True, 'b': True, 'c': True}))
    #     self.assertFalse(s.passes_qualifications({'a': True, 'b': True, 'c': False}))
    #
    #     # we do NOT match a full quota, so we pass
    #     self.assertTrue(s.passes_quotas({'a': True, 'b': True, 'd': False}))
    #     # We dont pass any
    #     self.assertFalse(s.passes_quotas({}))
    #     # we only pass a full quota
    #     self.assertFalse(s.passes_quotas({'d': True}))
    #     # we only dont pass a full quota, but we haven't passed any open
    #     self.assertFalse(s.passes_quotas({'d': False}))
    #     # we pass a quota, but also pass a full quota, so fail
    #     self.assertFalse(s.passes_quotas({'e': True, 'd': True}))
    #     # we pass a quota, but are unknown in a full quota, so fail
    #     self.assertFalse(s.passes_quotas({'e': True}))
    #
    #     # # Soft Pair
    #     self.assertEqual((True, set()), s.passes_qualifications_soft({'a': True, 'b': True, 'c': True}))
    #     self.assertEqual((False, set()), s.passes_qualifications_soft({'a': True, 'b': True, 'c': False}))
    #     self.assertEqual((None, set('c')), s.passes_qualifications_soft({'a': True, 'b': True, 'c': None}))
    #
    #     # we do NOT match a full quota, so we pass
    #     self.assertEqual((True, set()), s.passes_quotas_soft({'a': True, 'b': True, 'd': False}))
    #     # We dont pass any
    #     self.assertEqual((None, {'a', 'b', 'd', 'e'}), s.passes_quotas_soft({}))
    #     # we only pass a full quota
    #     self.assertEqual((False, set()), s.passes_quotas_soft({'d': True}))
    #     # we only dont pass a full quota, but we haven't passed any open
    #     self.assertEqual((None, {'a', 'b', 'e'}), s.passes_quotas_soft({'d': False}))
    #     # we pass a quota, but also pass a full quota, so fail
    #     self.assertEqual((False, set()), s.passes_quotas_soft({'e': True, 'd': True}))
    #     # we pass a quota, but are unknown in a full quota, so fail
    #     self.assertEqual((None, {'d'}), s.passes_quotas_soft({'e': True}))
    #
    #     self.assertEqual(True, s.determine_eligibility({'a': True, 'b': True, 'c': True, 'd': False}))
    #     self.assertEqual(False, s.determine_eligibility({'a': True, 'b': True, 'c': False, 'd': False}))
    #     self.assertEqual(False, s.determine_eligibility({'a': True, 'b': True, 'c': None, 'd': False}))
    #     self.assertEqual((True, set()), s.determine_eligibility_soft({'a': True, 'b': True, 'c': True, 'd': False}))
    #     self.assertEqual((False, set()), s.determine_eligibility_soft({'a': True, 'b': True, 'c': False, 'd': False}))
    #     self.assertEqual((None, set('c')), s.determine_eligibility_soft({'a': True, 'b': True, 'c': None,
    #     'd': False}))
    #     self.assertEqual((None, {'c', 'd'}), s.determine_eligibility_soft({'a': True, 'b': True, 'c': None,
    #     'd': None}))
