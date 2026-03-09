from typing import Any, Optional, Tuple

from generalresearch.models.thl.definitions import Status, StatusCode1
from generalresearch.wall_status_codes import lucid


def annotate_status_code(
    ext_status_code_1: str,
    ext_status_code_2: Optional[str] = None,
    ext_status_code_3: Optional[str] = None,
) -> Tuple[Status, StatusCode1, Optional[Any]]:
    return lucid.annotate_status_code(
        ext_status_code_1=ext_status_code_1,
        ext_status_code_2=ext_status_code_2,
        ext_status_code_3=ext_status_code_3,
    )
