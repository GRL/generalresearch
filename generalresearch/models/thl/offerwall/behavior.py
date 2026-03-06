from typing import Any, Dict, Literal

from pydantic import Field, BaseModel


class OfferWallBehavior(BaseModel):
    id: str = Field()
    name: str = Field()
    kwargs: Dict[str, Any] = Field(default_factory=dict)


OFFERWALL_BEHAVIOR_PRESETS = [
    OfferWallBehavior(
        id="0adc081e",
        name="Best for New Users",
        kwargs={
            "longfail_factor_adj": 1,
            "conversion_factor_adj": 1.5,
            "dropoffrate_factor_adj": 1,
        },
    ),
    OfferWallBehavior(
        id="626984a8", name="Dopamine Hit", kwargs={"conversion_factor_adj": 2}
    ),
    OfferWallBehavior(
        id="e3259520",
        name="Optimal Surveys!",
        kwargs={
            "longfail_factor_adj": 0,
            "conversion_factor_adj": 0,
            "dropoffrate_factor_adj": 0,
        },
    ),
    OfferWallBehavior(
        id="ffbd76b8",
        name="Low Frustration",
        kwargs={
            "longfail_factor_adj": 2,
            "conversion_factor_adj": 0.5,
            "dropoffrate_factor_adj": 2,
        },
    ),
]
OFFERWALL_BEHAVIOR_PRESETS_DICT = {x.id: x for x in OFFERWALL_BEHAVIOR_PRESETS}
OfferWallBehaviorsType = Literal["0adc081e", "626984a8", "e3259520", "ffbd76b8"]
