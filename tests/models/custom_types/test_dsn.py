from typing import Optional
from uuid import uuid4

import pytest
from pydantic import BaseModel, ValidationError, Field
from pydantic import MySQLDsn
from pydantic_core import Url

from generalresearch.models.custom_types import DaskDsn, SentryDsn


# --- Test Pydantic Models ---


class SettingsModel(BaseModel):
    dask: Optional["DaskDsn"] = Field(default=None)
    sentry: Optional["SentryDsn"] = Field(default=None)
    db: Optional["MySQLDsn"] = Field(default=None)


# --- Pytest themselves ---


class TestDaskDsn:

    def test_base(self):
        from dask.distributed import Client

        m = SettingsModel(dask="tcp://dask-scheduler.internal")

        assert m.dask.scheme == "tcp"
        assert m.dask.host == "dask-scheduler.internal"
        assert m.dask.port == 8786

        with pytest.raises(expected_exception=TypeError) as cm:
            Client(m.dask)
        assert "Scheduler address must be a string or a Cluster instance" in str(
            cm.value
        )

        # todo: this requires vpn connection. maybe do this part with a localhost dsn
        # client = Client(str(m.dask))
        # self.assertIsInstance(client, Client)

    def test_str(self):
        m = SettingsModel(dask="tcp://dask-scheduler.internal")
        assert isinstance(m.dask, Url)
        assert "tcp://dask-scheduler.internal:8786" == str(m.dask)

    def test_auth(self):
        with pytest.raises(expected_exception=ValidationError) as cm:
            SettingsModel(dask="tcp://test:password@dask-scheduler.internal")
        assert "User & Password are not supported" in str(cm.value)

        with pytest.raises(expected_exception=ValidationError) as cm:
            SettingsModel(dask="tcp://test:@dask-scheduler.internal")
        assert "User & Password are not supported" in str(cm.value)

        with pytest.raises(expected_exception=ValidationError) as cm:
            SettingsModel(dask="tcp://:password@dask-scheduler.internal")
        assert "User & Password are not supported" in str(cm.value)

    def test_invalid_schema(self):
        with pytest.raises(expected_exception=ValidationError) as cm:
            SettingsModel(dask="dask-scheduler.internal")
        assert "relative URL without a base" in str(cm.value)

        # I look forward to the day we use infiniband interfaces
        with pytest.raises(expected_exception=ValidationError) as cm:
            SettingsModel(dask="ucx://dask-scheduler.internal")
        assert "URL scheme should be 'tcp'" in str(cm.value)

    def test_port(self):
        m = SettingsModel(dask="tcp://dask-scheduler.internal")
        assert m.dask.port == 8786


class TestSentryDsn:
    def test_base(self):
        m = SettingsModel(
            sentry=f"https://{uuid4().hex}@12345.ingest.us.sentry.io/9876543"
        )

        assert m.sentry.scheme == "https"
        assert m.sentry.host == "12345.ingest.us.sentry.io"
        assert m.sentry.port == 443

    def test_str(self):
        test_url: str = f"https://{uuid4().hex}@12345.ingest.us.sentry.io/9876543"
        m = SettingsModel(sentry=test_url)
        assert isinstance(m.sentry, Url)
        assert test_url == str(m.sentry)

    def test_auth(self):
        with pytest.raises(expected_exception=ValidationError) as cm:
            SettingsModel(
                sentry="https://0123456789abc:password@12345.ingest.us.sentry.io/9876543"
            )
        assert "Sentry password is not supported" in str(cm.value)

        with pytest.raises(expected_exception=ValidationError) as cm:
            SettingsModel(sentry="https://test:@12345.ingest.us.sentry.io/9876543")
        assert "Sentry user key seems bad" in str(cm.value)

        with pytest.raises(expected_exception=ValidationError) as cm:
            SettingsModel(sentry="https://:password@12345.ingest.us.sentry.io/9876543")
        assert "Sentry URL requires a user key" in str(cm.value)

    def test_port(self):
        test_url: str = f"https://{uuid4().hex}@12345.ingest.us.sentry.io/9876543"
        m = SettingsModel(sentry=test_url)
        assert m.sentry.port == 443
