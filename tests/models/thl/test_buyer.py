from generalresearch.models import Source
from generalresearch.models.thl.survey.buyer import BuyerCountryStat


def test_buyer_country_stat():
    bcs = BuyerCountryStat(
        country_iso="us",
        source=Source.TESTING,
        code="123",
        task_count=100,
        conversion_alpha=40,
        conversion_beta=190,
        dropoff_alpha=20,
        dropoff_beta=50,
        long_fail_rate=1,
        loi_excess_ratio=1,
        user_report_coeff=1,
        recon_likelihood=0.05,
    )
    assert bcs.score
    print(bcs.score)
    print(bcs.conversion_p20)
    print(bcs.dropoff_p60)
