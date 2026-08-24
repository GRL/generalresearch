from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

# from generalresearch.config import
from generalresearch.models.custom_types import UUIDStr


def get_screenshot_fp(
    created_at: datetime,
    forensic_uuid: UUIDStr | UUID,
    grliq_archive_dir: Path = "/tmp",
    grliq_ss_dir_name: str = "canvas2html",
    create_dir_if_not_exists: bool = True,
) -> Path | None:
    assert created_at.tzinfo == UTC

    if isinstance(forensic_uuid, UUID):
        forensic_uuid = forensic_uuid.hex

    directory_path = os.path.join(
        grliq_archive_dir,
        grliq_ss_dir_name,
        created_at.strftime("%Y"),
        created_at.strftime("%m"),
        created_at.strftime("%d"),
    )

    if create_dir_if_not_exists:
        os.makedirs(directory_path, exist_ok=True)

    fp = Path(os.path.join(directory_path, f"{forensic_uuid}.png"))

    return fp
