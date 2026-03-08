from __future__ import annotations

from collections import namedtuple
from dataclasses import dataclass, fields
from functools import cached_property
from typing import Any, Dict, List, Optional

import numpy as np
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
)
from typing_extensions import Self

from generalresearch.models.custom_types import AwareDatetimeISO, IPvAnyAddressStr


class Bounds(namedtuple("BoundsBase", ["left", "top", "width", "height"])):
    __slots__ = ()

    @property
    def right(self):
        return self.left + self.width

    @property
    def bottom(self):
        return self.top + self.height


@dataclass(kw_only=True)
class Event:
    type: str
    # in microseconds, since page load (?)
    timeStamp: float
    # optional ID of the event target (e.g.: where the mouse is hovering)
    _elementId: Optional[str] = None
    # optional tag name of the event target
    _elementTagName: Optional[str] = None
    # extracted coordinates for the element being interacted with
    _elementBounds: Optional[Bounds] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Self:
        data = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        bounds = data.get("_elementBounds")
        if bounds is not None and not isinstance(bounds, Bounds):
            data = {**data, "_elementBounds": Bounds(**bounds)}
        return cls(**data)


@dataclass
class PointerMove(Event):
    # should always be 'pointermove'
    type: str

    # (mouse, touch, pen)
    pointerType: str

    # Coordinate relative to the screen
    screenX: float
    screenY: float

    # Coordinate relative to the document (unaffected by scrolling)
    pageX: float
    pageY: float

    # PageX/Y divided by the document Width/Height. This is calculated in JS
    # and sent, which it must be b/c we don't know the document width/height
    # at each time otherwise.
    normalizedX: float
    normalizedY: float


pointermove_keys = {f.name for f in fields(PointerMove)}


@dataclass
class MouseEvent(Event):
    """
    More general than PointerMove. To be used for handling touch events/mobile
    also, which don't generate pointermove events.
    """

    # should be {'pointerdown', 'pointerup', 'pointermove', 'click'}
    type: str

    # Type of input (mouse, touch, pen)
    pointerType: str

    # coordinate relative to the document (unaffected by scrolling)
    pageX: float
    pageY: float


@dataclass
class KeyboardEvent(Event):

    # should be {'keydown', 'input'}
    type: str

    # "insertText", "insertCompositionText", "deleteCompositionText",
    #   "insertFromComposition", "deleteContentBackward"
    inputType: Optional[str]

    # e.g., 'Enter', 'a', 'Backspace'
    key: Optional[str] = None

    # This is the actual text, if applicable
    data: Optional[str] = None

    @property
    def key_text(self):
        # if we get the input and keydown for a single char press, we don't need both
        return (
            f"<{self.key.upper()}>"
            if self.key
            and self.key.lower() not in {"unidentified", ""}
            and len(self.key) > 1
            else None
        )

    @property
    def input_type_text(self):
        return f"<{self.inputType.upper()}>" if self.inputType else None

    @property
    def text(self):
        return self.data or self.key_text or self.input_type_text or ""


class TimingDataSummary(BaseModel):
    """
    Summarizes the pings from a single TimingData
    (measurements from a single websocket connection / session, for one user
    on one IP)
    """

    count: NonNegativeInt = Field(description="After filtering out outliers")
    outlier_count: NonNegativeInt = Field()
    outlier_frac: NonNegativeFloat = Field(ge=0, le=1)

    median_log_rtt: PositiveFloat = Field()
    mean_log_rtt: PositiveFloat = Field()
    std_log_rtt: PositiveFloat = Field()

    median_rtt: PositiveFloat = Field()
    mean_rtt: PositiveFloat = Field()
    std_rtt: PositiveFloat = Field()


class TimingData(BaseModel):
    """
    Stores collected RTTs from websocket pings.
    todo: can also store bandwidth info collected from router
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    client_rtts: List[float] = Field()
    server_rtts: List[float] = Field()

    # Have to be optional for backwards-compatibility, but should always be set.
    started_at: Optional[AwareDatetimeISO] = Field(default=None)
    ended_at: Optional[AwareDatetimeISO] = Field(default=None)
    client_ip: Optional[IPvAnyAddressStr] = Field(
        description="This comes from the websocket request's headers",
        examples=["72.39.217.116"],
        default=None,
    )
    server_hostname: Optional[str] = Field(
        description="The hostname of the server that handled this request",
        examples=["grliq-web-0"],
        default=None,
    )

    @property
    def server_location(self) -> str:
        # TODO: when we have more locations ...
        return (
            "fremont_ca"
            if self.server_hostname in {"grliq-web-0", "grliq-web-1"}
            else "fremont_ca"
        )

    @property
    def has_data(self):
        return len(self.client_rtts) > 0 and len(self.server_rtts) > 0

    def filter_rtts(self, rtts: List[float]) -> List[float]:
        # Skip the first 5 pings, unless we have <10 pings, then get the last
        #   5 instead.
        # The first couple pings are usually outliers as they are running
        # when a lot of initial JS is also running.
        if len(self.client_rtts) >= 10:
            rtts = rtts[5:]
        else:
            rtts = rtts[-5:]

        return rtts

    @cached_property
    def client_rtts_filtered(self):
        return self.filter_rtts(self.client_rtts)

    @property
    def client_rtt_mean(self):
        rtts = self.client_rtts_filtered
        return sum(rtts) / len(rtts)

    @cached_property
    def server_rtts_filtered(self):
        return self.filter_rtts(self.server_rtts)

    @property
    def server_rtt_mean(self):
        rtts = self.server_rtts_filtered
        return sum(rtts) / len(rtts)

    @property
    def filtered_rtts(self):
        return self.server_rtts_filtered + self.client_rtts_filtered

    @property
    def cleaned_rtts(self):
        # Trim outliers
        rtts = np.array(self.filtered_rtts)
        rtts = rtts[(rtts > 3) & (rtts < 1000)]
        if rtts.size > 0:
            p5, p95 = np.percentile(rtts, [5, 95])
            rtts = rtts[(rtts >= p5) & (rtts <= p95)]
        return rtts

    @property
    def summarize(self) -> Optional[TimingDataSummary]:
        if len(self.filtered_rtts) < 5:
            return None

        orig_len = len(self.filtered_rtts)
        rtts = np.array(self.cleaned_rtts)
        if len(rtts) < 5:
            # We started with 5 or more observations, but removed enough so that
            #   we have < 5 now. This is probably a signal of something
            return None

        log_rtts = np.log(rtts)

        return TimingDataSummary(
            count=len(rtts),
            outlier_count=orig_len - len(rtts),
            outlier_frac=(orig_len - len(rtts)) / orig_len,
            median_rtt=float(np.median(rtts)),
            mean_rtt=float(np.mean(rtts)),
            std_rtt=float(np.std(rtts)),
            mean_log_rtt=float(np.mean(log_rtts)),
            median_log_rtt=float(np.median(log_rtts)),
            std_log_rtt=float(np.std(log_rtts)),
        )
