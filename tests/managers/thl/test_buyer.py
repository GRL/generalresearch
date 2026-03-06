from generalresearch.models import Source


class TestBuyer:

    def test(
        self,
        delete_buyers_surveys,
        buyer_manager,
    ):

        bs = buyer_manager.bulk_get_or_create(source=Source.TESTING, codes=["a", "b"])
        assert len(bs) == 2
        buyer_a = bs[0]
        assert buyer_a.id is not None
        bs2 = buyer_manager.bulk_get_or_create(source=Source.TESTING, codes=["a", "c"])
        assert len(bs2) == 2
        buyer_a2 = bs2[0]
        buyer_c = bs2[1]
        # a isn't created again
        assert buyer_a == buyer_a2
        assert bs2[0].id is not None

        # and its cached
        assert buyer_c.id == buyer_manager.source_code_pk[f"{Source.TESTING.value}:c"]
