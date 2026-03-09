from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from generalresearch.config import GRLBaseSettings
    from generalresearch.grliq.models.forensic_data import GrlIqData


@pytest.fixture(scope="function")
def mnt_grliq_archive_dir(settings: "GRLBaseSettings") -> Optional[str]:
    return settings.mnt_grliq_archive_dir


@pytest.fixture(scope="function")
def grliq_data() -> "GrlIqData":
    from generalresearch.grliq.managers import DUMMY_GRLIQ_DATA
    from generalresearch.grliq.models.forensic_data import GrlIqData

    g: GrlIqData = DUMMY_GRLIQ_DATA[1]["data"]

    g.id = None
    g.uuid = uuid4().hex
    g.created_at = datetime.now(tz=timezone.utc)
    g.timestamp = g.created_at - timedelta(seconds=10)
    return g
