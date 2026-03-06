import pandas as pd
from pandera import DataFrameSchema, Column, Check, Index

from generalresearch.locales import Localelator
from generalresearch.models import Source

COUNTRY_ISOS = Localelator().get_all_countries()
kosovo = "xk"
COUNTRY_ISOS.add(kosovo)

SURVEY_STATS_COLS = [
    "PRESCREEN_CONVERSION.alpha",
    "PRESCREEN_CONVERSION.beta",
    "CONVERSION.alpha",
    "CONVERSION.beta",
    "COMPLETION_TIME.mu",
    "COMPLETION_TIME.sigma",
    "LONG_FAIL.value",
    "USER_REPORT_COEFF.value",
    "RECON_LIKELIHOOD.value",
    "DROPOFF_RATE.alpha",
    "DROPOFF_RATE.beta",
    "IS_MOBILE_ELIGIBLE.alpha",
    "IS_MOBILE_ELIGIBLE.beta",
    "IS_DESKTOP_ELIGIBLE.alpha",
    "IS_DESKTOP_ELIGIBLE.beta",
    "IS_TABLET_ELIGIBLE.alpha",
    "IS_TABLET_ELIGIBLE.beta",
    "cpi",
    "country_iso",
    "is_recontact",
    "score_x0",
    "score_x1",
    "buyer_id",
    "complete_too_fast_cutoff",
]

# e.g. The parameters for a beta distribution are just real numbers > 0, but
# in practice, if the numbers are "very" large, something is def wrong.
# Defining "very" somewhat arbitrarily here.
SUSPICIOUSLY_LARGE_NUMBER = (2**32 / 2) - 1  # 2147483647

PositiveRealNumber = Column(
    dtype=float,
    nullable=False,
    checks=Check.between(
        min_value=0, max_value=SUSPICIOUSLY_LARGE_NUMBER, include_min=False
    ),
)
NonNegativeRealNumber = Column(
    dtype=float,
    nullable=False,
    checks=Check.between(
        min_value=0, max_value=SUSPICIOUSLY_LARGE_NUMBER, include_min=True
    ),
)
RealNumber = Column(
    dtype=float,
    nullable=False,
    checks=Check.between(
        min_value=SUSPICIOUSLY_LARGE_NUMBER, max_value=SUSPICIOUSLY_LARGE_NUMBER
    ),
)
# Real number between 0 and 1 inclusive.
UnitInterval = Column(
    dtype=float,
    nullable=False,
    checks=Check.between(min_value=0, max_value=1),
)

SID_CHECKS = [
    Check.str_length(min_value=3, max_value=67),
    Check.str_matches("^[a-z]{1,2}\:[A-Za-z0-9]+"),
    Check(
        lambda x: len(set(x.str.split(":").str[0])) == 1,
        error="the sources must all be the same",
    ),
]

SurveyStatSchema = DataFrameSchema(
    index=Index(
        name="sid",
        description="CURIE format (source:task_id)",
        dtype=str,
        unique=True,
        checks=SID_CHECKS,
    ),
    columns={
        "source": Column(
            dtype=str, nullable=False, checks=[Check.isin([e.value for e in Source])]
        ),
        "task_id": Column(dtype=str, nullable=False, checks=[Check.str_length(1, 64)]),
        "PRESCREEN_CONVERSION.alpha": PositiveRealNumber,
        "PRESCREEN_CONVERSION.beta": PositiveRealNumber,
        "CONVERSION.alpha": PositiveRealNumber,
        "CONVERSION.beta": PositiveRealNumber,
        # Normal distribution, so mu is real number, but this represents the
        # completion time, so it has to be positive. We can restrict it more
        # in that me are never going to predict time longer than ~~ 2
        # hours (np.log(120*60)) or <= 0 sec (np.log(1) = 0)
        "COMPLETION_TIME.mu": Column(
            dtype=float,
            nullable=False,
            checks=Check.between(min_value=1, max_value=10, include_min=False),
        ),
        "COMPLETION_TIME.sigma": Column(
            dtype=float,
            nullable=False,
            checks=Check.between(min_value=0, max_value=10, include_min=False),
        ),
        # this should be much less than 10... I think check
        "LONG_FAIL.value": Column(
            dtype=float,
            nullable=False,
            checks=Check.between(min_value=0, max_value=10, include_min=False),
        ),
        "USER_REPORT_COEFF.value": UnitInterval,
        "RECON_LIKELIHOOD.value": UnitInterval,
        "DROPOFF_RATE.alpha": PositiveRealNumber,
        "DROPOFF_RATE.beta": PositiveRealNumber,
        "IS_MOBILE_ELIGIBLE.alpha": PositiveRealNumber,
        "IS_MOBILE_ELIGIBLE.beta": PositiveRealNumber,
        "IS_DESKTOP_ELIGIBLE.alpha": PositiveRealNumber,
        "IS_DESKTOP_ELIGIBLE.beta": PositiveRealNumber,
        "IS_TABLET_ELIGIBLE.alpha": PositiveRealNumber,
        "IS_TABLET_ELIGIBLE.beta": PositiveRealNumber,
        "cpi": Column(
            dtype=float,
            checks=Check.between(min_value=0, max_value=1_000),
            nullable=False,
        ),
        "country_iso": Column(
            dtype=str,
            checks=[
                Check.str_length(min_value=1, max_value=2),
                Check.isin(COUNTRY_ISOS),  # 2 letter, lowercase
            ],
            nullable=True,
        ),
        "is_recontact": Column(dtype=bool),
        "score_x0": NonNegativeRealNumber,
        "score_x1": NonNegativeRealNumber,
        "buyer_id": Column(
            dtype=str,
            checks=[Check.str_length(min_value=1, max_value=32)],
            nullable=True,
        ),
        "complete_too_fast_cutoff": Column(
            dtype=float,
            nullable=False,
            checks=Check.between(min_value=0, max_value=120 * 60, include_min=False),
        ),
        "created": Column(
            dtype=pd.DatetimeTZDtype(tz="UTC"), nullable=True, required=False
        ),
    },
    checks=[],
    coerce=True,
)
