import pytest
from pydantic import MySQLDsn

from generalresearch.managers.marketplace.user_pid import UserPidMultiManager
from generalresearch.sql_helper import SqlHelper
from generalresearch.managers.cint.user_pid import CintUserPidManager
from generalresearch.managers.dynata.user_pid import DynataUserPidManager
from generalresearch.managers.innovate.user_pid import InnovateUserPidManager
from generalresearch.managers.morning.user_pid import MorningUserPidManager

# from generalresearch.managers.precision import PrecisionUserPidManager
from generalresearch.managers.prodege.user_pid import ProdegeUserPidManager
from generalresearch.managers.repdata.user_pid import RepdataUserPidManager
from generalresearch.managers.sago.user_pid import SagoUserPidManager
from generalresearch.managers.spectrum.user_pid import SpectrumUserPidManager

dsn = ""


class TestCintUserPidManager:

    def test_filter(self):
        m = CintUserPidManager(SqlHelper(MySQLDsn(dsn + "thl-cint")))

        with pytest.raises(expected_exception=AssertionError) as excinfo:
            m.filter()
        assert str(excinfo.value) == "Must pass ONE of user_ids, pids"

        with pytest.raises(expected_exception=AssertionError) as excinfo:
            m.filter(user_ids=[1, 2, 3], pids=["ed5b47c8551d453d985501391f190d3f"])
        assert str(excinfo.value) == "Must pass ONE of user_ids, pids"

        # pids get .hex before and after
        assert m.filter(pids=["ed5b47c8551d453d985501391f190d3f"]) == m.filter(
            pids=["ed5b47c8-551d-453d-9855-01391f190d3f"]
        )

        # user_ids and pids are for the same 3 users
        res1 = m.filter(user_ids=[61586871, 61458915, 61390116])
        res2 = m.filter(
            pids=[
                "ed5b47c8551d453d985501391f190d3f",
                "7e640732c59f43d1b7c00137ab66600c",
                "5160aeec9c3b4dbb85420128e6da6b5a",
            ]
        )
        assert res1 == res2


class TestUserPidMultiManager:

    def test_filter(self):
        managers = [
            CintUserPidManager(SqlHelper(MySQLDsn(dsn + "thl-cint"))),
            DynataUserPidManager(SqlHelper(MySQLDsn(dsn + "thl-dynata"))),
            InnovateUserPidManager(SqlHelper(MySQLDsn(dsn + "thl-innovate"))),
            MorningUserPidManager(SqlHelper(MySQLDsn(dsn + "thl-morning"))),
            ProdegeUserPidManager(SqlHelper(MySQLDsn(dsn + "thl-prodege"))),
            RepdataUserPidManager(SqlHelper(MySQLDsn(dsn + "thl-repdata"))),
            SagoUserPidManager(SqlHelper(MySQLDsn(dsn + "thl-sago"))),
            SpectrumUserPidManager(SqlHelper(MySQLDsn(dsn + "thl-spectrum"))),
        ]
        m = UserPidMultiManager(sql_helper=SqlHelper(MySQLDsn(dsn)), managers=managers)
        res = m.filter(user_ids=[1])
        assert len(res) == len(managers)

        res = m.filter(user_ids=[1, 2, 3])
        assert len(res) > len(managers)
