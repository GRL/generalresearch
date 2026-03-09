from typing import Optional, Tuple

from generalresearch.models import Source
from generalresearch.models.thl.definitions import Status, StatusCode1
from generalresearch.models.thl.session import Wall
from generalresearch.wall_status_codes import (
    cint,
    dynata,
    fullcircle,
    innovate,
    lucid,
    morning,
    pollfish,
    precision,
    prodege,
    repdata,
    sago,
    spectrum,
)


def annotate_status_code(
    source: Source,
    ext_status_code_1: str,
    ext_status_code_2: Optional[str] = None,
    ext_status_code_3: Optional[str] = None,
) -> Tuple[Status, Optional[StatusCode1], Optional[str]]:
    """
    :params ext_status_code_1: marketplace-dependent code
    :params ext_status_code_2: marketplace-dependent code
    :params ext_status_code_3: marketplace-dependent code

    returns: (status, status_code_1, status_code_2)
    """
    if source == Source.DALIA:
        return Status.FAIL, StatusCode1.UNKNOWN, None
    if source == Source.PULLEY:
        return Status.FAIL, StatusCode1.UNKNOWN, None

    return {
        Source.CINT: cint.annotate_status_code,
        Source.DYNATA: dynata.annotate_status_code,
        Source.FULL_CIRCLE: fullcircle.annotate_status_code,
        Source.INNOVATE: innovate.annotate_status_code,
        Source.LUCID: lucid.annotate_status_code,
        Source.MORNING_CONSULT: morning.annotate_status_code,
        Source.POLLFISH: pollfish.annotate_status_code,
        Source.PRECISION: precision.annotate_status_code,
        Source.PRODEGE: prodege.annotate_status_code,
        Source.SAGO: sago.annotate_status_code,
        Source.SPECTRUM: spectrum.annotate_status_code,
        Source.REPDATA: repdata.annotate_status_code,
    }[source](ext_status_code_1, ext_status_code_2, ext_status_code_3)


def stop_marketplace_session(wall: Wall) -> bool:
    if wall.source == Source.DYNATA:
        return dynata.stop_marketplace_session(
            wall.status_code_1, wall.ext_status_code_1
        )

    elif wall.status_code_1 in {
        StatusCode1.PS_QUALITY,
        StatusCode1.BUYER_QUALITY_FAIL,
        StatusCode1.PS_BLOCKED,
        StatusCode1.UNKNOWN,
    }:
        return True

    return False


def is_soft_fail(wall: Wall) -> bool:
    #  Assuming this is already a fail... ignored otherwise
    if wall.source == Source.FULL_CIRCLE:
        # todo: this may not have been set when this is called??
        return fullcircle.is_soft_fail(wall.elapsed)

    elif wall.status_code_1 in {
        StatusCode1.BUYER_FAIL,
        StatusCode1.BUYER_QUALITY_FAIL,
        StatusCode1.UNKNOWN,
    }:
        return False

    return True


# def stop_marketplace_session(source: Source, status_code_1: StatusCode1, ext_status_code_1: Optional[str] = None):
#     # Each marketplace can have their own version, or use this one as the default
#     if source == Source.DYNATA:
#         return dynata.stop_marketplace_session(status_code_1, ext_status_code_1)
#     if status_code_1 in {StatusCode1.PS_QUALITY, StatusCode1.BUYER_QUALITY_FAIL, StatusCode1.PS_BLOCKED}:
#         return True
#     return False
#
#
# def is_soft_fail(source: Source, status_code_1: StatusCode1, elapsed: Optional[timedelta] = None):
#     # Each marketplace can have their own version, or use this one as the default
#     #  Assuming this is already a fail... ignored otherwise
#     if source == Source.FULL_CIRCLE:
#         assert elapsed is not None
#         return fullcircle.is_soft_fail(elapsed)
#     if status_code_1 in {StatusCode1.BUYER_FAIL, StatusCode1.BUYER_QUALITY_FAIL}:
#         return False
#     return True
