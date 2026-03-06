import logging
from datetime import datetime, timezone
from typing import Optional

import pytest
import pytz
from pydantic import BaseModel, ValidationError, Field

from generalresearch.models.custom_types import AwareDatetimeISO

logger = logging.getLogger()


class AwareDatetimeISOModel(BaseModel):
    dt_optional: Optional[AwareDatetimeISO] = Field(default=None)
    dt: AwareDatetimeISO


class TestAwareDatetimeISO:
    def test_str(self):
        dt = "2023-10-10T01:01:01.0Z"
        t = AwareDatetimeISOModel(dt=dt, dt_optional=dt)
        AwareDatetimeISOModel.model_validate_json(t.model_dump_json())

        t = AwareDatetimeISOModel(dt=dt, dt_optional=None)
        AwareDatetimeISOModel.model_validate_json(t.model_dump_json())

    def test_dt(self):
        dt = datetime(2023, 10, 10, 1, 1, 1, tzinfo=timezone.utc)
        t = AwareDatetimeISOModel(dt=dt, dt_optional=dt)
        AwareDatetimeISOModel.model_validate_json(t.model_dump_json())

        t = AwareDatetimeISOModel(dt=dt, dt_optional=None)
        AwareDatetimeISOModel.model_validate_json(t.model_dump_json())

        dt = datetime(2023, 10, 10, 1, 1, 1, microsecond=123, tzinfo=timezone.utc)
        t = AwareDatetimeISOModel(dt=dt, dt_optional=dt)
        AwareDatetimeISOModel.model_validate_json(t.model_dump_json())

        t = AwareDatetimeISOModel(dt=dt, dt_optional=None)
        AwareDatetimeISOModel.model_validate_json(t.model_dump_json())

    def test_no_tz(self):
        dt = datetime(2023, 10, 10, 1, 1, 1)

        with pytest.raises(expected_exception=ValidationError):
            AwareDatetimeISOModel(dt=dt, dt_optional=None)

        dt = "2023-10-10T01:01:01.0"
        with pytest.raises(expected_exception=ValidationError):
            AwareDatetimeISOModel(dt=dt, dt_optional=None)

    def test_non_utc_tz(self):
        dt = datetime(
            year=2023,
            month=10,
            day=10,
            hour=1,
            second=1,
            minute=1,
            tzinfo=pytz.timezone("US/Central"),
        )

        with pytest.raises(expected_exception=ValidationError):
            AwareDatetimeISOModel(dt=dt, dt_optional=dt)

    def test_invalid_format(self):
        dt = "2023-10-10T01:01:01Z"
        with pytest.raises(expected_exception=ValidationError):
            AwareDatetimeISOModel(dt=dt, dt_optional=dt)

        dt = "2023-10-10T01:01:01"
        with pytest.raises(expected_exception=ValidationError):
            AwareDatetimeISOModel(dt=dt, dt_optional=dt)
        dt = "2023-10-10"
        with pytest.raises(expected_exception=ValidationError):
            AwareDatetimeISOModel(dt=dt, dt_optional=dt)

    def test_required(self):
        dt = "2023-10-10T01:01:01.0Z"
        with pytest.raises(expected_exception=ValidationError):
            AwareDatetimeISOModel(dt=None, dt_optional=dt)
