from typing import Optional

from generalresearch.wall_status_codes import lucid


def annotate_status_code(
    ext_status_code_1: str,
    ext_status_code_2: Optional[str] = None,
    ext_status_code_3: Optional[str] = None,
):
    return lucid.annotate_status_code(
        ext_status_code_1=ext_status_code_1,
        ext_status_code_2=ext_status_code_2,
        ext_status_code_3=ext_status_code_3,
    )
