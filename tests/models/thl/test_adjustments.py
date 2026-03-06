from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from generalresearch.models import Source
from generalresearch.models.thl.session import (
    Wall,
    Status,
    StatusCode1,
    WallAdjustedStatus,
    SessionAdjustedStatus,
)

started1 = datetime(2023, 1, 1, tzinfo=timezone.utc)
started2 = datetime(2023, 1, 1, 0, 10, 0, tzinfo=timezone.utc)
finished1 = started1 + timedelta(minutes=10)
finished2 = started2 + timedelta(minutes=10)

adj_ts = datetime(2023, 2, 2, tzinfo=timezone.utc)
adj_ts2 = datetime(2023, 2, 3, tzinfo=timezone.utc)
adj_ts3 = datetime(2023, 2, 4, tzinfo=timezone.utc)


class TestProductAdjustments:

    @pytest.mark.parametrize("payout", [".6", "1", "1.8", "2", "500.0000"])
    def test_determine_bp_payment_no_rounding(self, product_factory, payout):
        p1 = product_factory(commission_pct=Decimal("0.05"))
        res = p1.determine_bp_payment(thl_net=Decimal(payout))
        assert isinstance(res, Decimal)
        assert res == Decimal(payout) * Decimal("0.95")

    @pytest.mark.parametrize("payout", [".01", ".05", ".5"])
    def test_determine_bp_payment_rounding(self, product_factory, payout):
        p1 = product_factory(commission_pct=Decimal("0.05"))
        res = p1.determine_bp_payment(thl_net=Decimal(payout))
        assert isinstance(res, Decimal)
        assert res != Decimal(payout) * Decimal("0.95")


class TestSessionAdjustments:

    def test_status_complete(self, session_factory, user):
        # Completed Session with 2 wall events
        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpi=Decimal(1),
            final_status=Status.COMPLETE,
            started=started1,
        )

        # Confirm only the last Wall Event is a complete
        assert not s1.wall_events[0].status == Status.COMPLETE
        assert s1.wall_events[1].status == Status.COMPLETE

        # Confirm the Session is marked as finished and the simple brokerage
        #   payout calculation is correct.
        status, status_code_1 = s1.determine_session_status()
        assert status == Status.COMPLETE
        assert status_code_1 == StatusCode1.COMPLETE


class TestAdjustments:

    def test_finish_with_status(self, session_factory, user, session_manager):
        # Completed Session with 2 wall events
        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpi=Decimal(1),
            final_status=Status.COMPLETE,
            started=started1,
        )

        status, status_code_1 = s1.determine_session_status()
        payout = user.product.determine_bp_payment(Decimal(1))
        session_manager.finish_with_status(
            session=s1,
            status=status,
            status_code_1=status_code_1,
            payout=payout,
            finished=finished2,
        )

        assert Decimal("0.95") == payout

    def test_never_adjusted(self, session_factory, user, session_manager):
        s1 = session_factory(
            user=user,
            wall_count=5,
            wall_req_cpi=Decimal(1),
            final_status=Status.COMPLETE,
            started=started1,
        )

        session_manager.finish_with_status(
            session=s1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            payout=Decimal("0.95"),
            finished=finished2,
        )

        # Confirm walls and Session are never adjusted in anyway
        for w in s1.wall_events:
            w: Wall
            assert w.adjusted_status is None
            assert w.adjusted_timestamp is None
            assert w.adjusted_cpi is None

        assert s1.adjusted_status is None
        assert s1.adjusted_payout is None
        assert s1.adjusted_timestamp is None

    def test_adjustment_wall_values(
        self, session_factory, user, session_manager, wall_manager
    ):
        # Completed Session with 2 wall events
        s1 = session_factory(
            user=user,
            wall_count=5,
            wall_req_cpi=Decimal(1),
            final_status=Status.COMPLETE,
            started=started1,
        )

        session_manager.finish_with_status(
            session=s1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            payout=Decimal("0.95"),
            finished=finished2,
        )

        # Change the last wall event to a Failure
        w: Wall = s1.wall_events[-1]
        wall_manager.adjust_status(
            wall=w,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_timestamp=adj_ts,
        )

        # Original Session and Wall status is still the same, but the Adjusted
        #   values have changed
        assert s1.status == Status.COMPLETE
        assert s1.adjusted_status is None
        assert s1.adjusted_timestamp is None
        assert s1.adjusted_payout is None
        assert s1.adjusted_user_payout is None

        assert w.status == Status.COMPLETE
        assert w.status_code_1 == StatusCode1.COMPLETE
        assert w.adjusted_status == WallAdjustedStatus.ADJUSTED_TO_FAIL
        assert w.adjusted_cpi == Decimal(0)
        assert w.adjusted_timestamp == adj_ts

        # Because the Product doesn't have the Wallet mode enabled, the
        #   user_payout fields should always be None
        assert not user.product.user_wallet_config.enabled
        assert s1.adjusted_user_payout is None

    def test_adjustment_session_values(
        self, wall_manager, session_manager, session_factory, user
    ):
        # Completed Session with 2 wall events
        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpi=Decimal(1),
            wall_source=Source.DYNATA,
            final_status=Status.COMPLETE,
            started=started1,
        )

        session_manager.finish_with_status(
            session=s1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            payout=Decimal("0.95"),
            finished=finished2,
        )

        # Change the last wall event to a Failure
        wall_manager.adjust_status(
            wall=s1.wall_events[-1],
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_timestamp=adj_ts,
        )

        # Refresh the Session with the new Wall Adjustment considerations,
        session_manager.adjust_status(session=s1)
        assert s1.status == Status.COMPLETE  # Original status should remain
        assert s1.adjusted_status == SessionAdjustedStatus.ADJUSTED_TO_FAIL
        assert s1.adjusted_payout == Decimal(0)
        assert s1.adjusted_timestamp == adj_ts

        # Because the Product doesn't have the Wallet mode enabled, the
        #   user_payout fields should always be None
        assert not user.product.user_wallet_config.enabled
        assert s1.adjusted_user_payout is None

    def test_double_adjustment_session_values(
        self, wall_manager, session_manager, session_factory, user
    ):
        # Completed Session with 2 wall events
        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpi=Decimal(1),
            final_status=Status.COMPLETE,
            started=started1,
        )

        session_manager.finish_with_status(
            session=s1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            payout=Decimal("0.95"),
            finished=finished2,
        )

        # Change the last wall event to a Failure
        w: Wall = s1.wall_events[-1]
        wall_manager.adjust_status(
            wall=w,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_timestamp=adj_ts,
        )

        # Refresh the Session with the new Wall Adjustment considerations,
        session_manager.adjust_status(session=s1)

        # Let's take that back again! Buyers love to do this.
        # So now we're going to "un-reconcile" the last Wall Event which has
        # already gone from a Complete >> Failure
        wall_manager.adjust_status(
            wall=w, adjusted_status=None, adjusted_timestamp=adj_ts2
        )
        assert w.adjusted_status is None
        assert w.adjusted_cpi is None
        assert w.adjusted_timestamp == adj_ts2

        # Once the wall was unreconciled, "refresh" the Session again
        assert s1.adjusted_status is not None
        session_manager.adjust_status(session=s1)
        assert s1.adjusted_status is None
        assert s1.adjusted_payout is None
        assert s1.adjusted_timestamp == adj_ts2
        assert s1.adjusted_user_payout is None

    def test_double_adjustment_sm_vs_db_values(
        self, wall_manager, session_manager, session_factory, user
    ):
        # Completed Session with 2 wall events
        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpi=Decimal(1),
            wall_source=Source.DYNATA,
            final_status=Status.COMPLETE,
            started=started1,
        )

        session_manager.finish_with_status(
            session=s1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            payout=Decimal("0.95"),
            finished=finished2,
        )

        # Change the last wall event to a Failure
        wall_manager.adjust_status(
            wall=s1.wall_events[-1],
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_timestamp=adj_ts,
        )

        # Refresh the Session with the new Wall Adjustment considerations,
        session_manager.adjust_status(session=s1)

        # Let's take that back again! Buyers love to do this.
        # So now we're going to "un-reconcile" the last Wall Event which has
        # already gone from a Complete >> Failure
        # Once the wall was unreconciled, "refresh" the Session again
        wall_manager.adjust_status(
            wall=s1.wall_events[-1], adjusted_status=None, adjusted_timestamp=adj_ts2
        )
        session_manager.adjust_status(session=s1)

        # Confirm that the sessions wall attributes are still aligned with
        # what comes back directly from the database
        db_wall_events = wall_manager.get_wall_events(session_id=s1.id)
        for idx in range(len(s1.wall_events)):
            w_sm: Wall = s1.wall_events[idx]
            w_db: Wall = db_wall_events[idx]

            assert w_sm.uuid == w_db.uuid
            assert w_sm.session_id == w_db.session_id
            assert w_sm.status == w_db.status
            assert w_sm.status_code_1 == w_db.status_code_1
            assert w_sm.status_code_2 == w_db.status_code_2

            assert w_sm.elapsed == w_db.elapsed

            # Decimal("1.000000") vs Decimal(1) - based on mysql or postgres
            assert pytest.approx(w_sm.cpi) == w_db.cpi
            assert pytest.approx(w_sm.req_cpi) == w_db.req_cpi

            assert w_sm.model_dump_json(
                exclude={"cpi", "req_cpi"}
            ) == w_db.model_dump_json(exclude={"cpi", "req_cpi"})

    def test_double_adjustment_double_completes(
        self, wall_manager, session_manager, session_factory, user
    ):
        # Completed Session with 2 wall events
        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpi=Decimal(2),
            wall_source=Source.DYNATA,
            final_status=Status.COMPLETE,
            started=started1,
        )

        session_manager.finish_with_status(
            session=s1,
            status=Status.COMPLETE,
            status_code_1=StatusCode1.COMPLETE,
            payout=Decimal("0.95"),
            finished=finished2,
        )

        # Change the last wall event to a Failure
        wall_manager.adjust_status(
            wall=s1.wall_events[-1],
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_timestamp=adj_ts,
        )

        # Refresh the Session with the new Wall Adjustment considerations,
        session_manager.adjust_status(session=s1)

        # Let's take that back again! Buyers love to do this.
        # So now we're going to "un-reconcile" the last Wall Event which has
        # already gone from a Complete >> Failure
        # Once the wall was unreconciled, "refresh" the Session again
        wall_manager.adjust_status(
            wall=s1.wall_events[-1], adjusted_status=None, adjusted_timestamp=adj_ts2
        )
        session_manager.adjust_status(session=s1)

        # Reassign them - we already validated they're equal in previous
        #    tests so this is safe to do.
        s1.wall_events = wall_manager.get_wall_events(session_id=s1.id)

        # The First Wall event was originally a Failure, now let's also set
        # that as a complete, so now both Wall Events will b a
        # complete (Fail >> Adj to Complete, Complete >> Adj to Fail >> Adj to Complete)
        w1: Wall = s1.wall_events[0]
        assert w1.status == Status.FAIL
        assert w1.adjusted_status is None
        assert w1.adjusted_cpi is None
        assert w1.adjusted_timestamp is None

        wall_manager.adjust_status(
            wall=w1,
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_timestamp=adj_ts3,
        )

        assert w1.status == Status.FAIL  # original status doesn't change
        assert w1.adjusted_status == WallAdjustedStatus.ADJUSTED_TO_COMPLETE
        assert w1.adjusted_cpi == w1.cpi
        assert w1.adjusted_timestamp == adj_ts3

        session_manager.adjust_status(s1)
        assert SessionAdjustedStatus.PAYOUT_ADJUSTMENT == s1.adjusted_status
        assert Decimal("3.80") == s1.adjusted_payout
        assert s1.adjusted_user_payout is None
        assert adj_ts3 == s1.adjusted_timestamp

    def test_complete_to_fail(
        self, session_factory, user, session_manager, wall_manager, utc_hour_ago
    ):
        s1 = session_factory(
            user=user,
            wall_count=1,
            wall_req_cpi=Decimal("1"),
            final_status=Status.COMPLETE,
            started=utc_hour_ago,
        )

        status, status_code_1 = s1.determine_session_status()
        assert status == Status.COMPLETE

        thl_net = Decimal(sum(w.cpi for w in s1.wall_events if w.is_visible_complete()))
        payout = user.product.determine_bp_payment(thl_net=thl_net)

        session_manager.finish_with_status(
            session=s1,
            status=status,
            status_code_1=status_code_1,
            finished=utc_hour_ago + timedelta(minutes=10),
            payout=payout,
            user_payout=None,
        )

        w1 = s1.wall_events[0]
        w1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=adj_ts,
        )
        assert w1.adjusted_status == WallAdjustedStatus.ADJUSTED_TO_FAIL
        assert w1.adjusted_cpi == Decimal(0)

        new_status, new_payout, new_user_payout = s1.determine_new_status_and_payouts()
        assert Status.FAIL == new_status
        assert Decimal(0) == new_payout

        assert not user.product.user_wallet_config.enabled
        assert new_user_payout is None

        s1.adjust_status()
        assert SessionAdjustedStatus.ADJUSTED_TO_FAIL == s1.adjusted_status
        assert Decimal(0) == s1.adjusted_payout
        assert not user.product.user_wallet_config.enabled
        assert s1.adjusted_user_payout is None

        # cpi adjustment
        w1.update(
            adjusted_status=WallAdjustedStatus.CPI_ADJUSTMENT,
            adjusted_cpi=Decimal("0.69"),
            adjusted_timestamp=adj_ts,
        )
        assert WallAdjustedStatus.CPI_ADJUSTMENT == w1.adjusted_status
        assert Decimal("0.69") == w1.adjusted_cpi
        new_status, new_payout, new_user_payout = s1.determine_new_status_and_payouts()
        assert Status.COMPLETE == new_status
        assert Decimal("0.66") == new_payout

        assert not user.product.user_wallet_config.enabled
        # assert Decimal("0.33") == new_user_payout
        assert new_user_payout is None

        s1.adjust_status()
        assert SessionAdjustedStatus.PAYOUT_ADJUSTMENT == s1.adjusted_status
        assert Decimal("0.66") == s1.adjusted_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal("0.33") == s1.adjusted_user_payout
        assert s1.adjusted_user_payout is None

        # adjust cpi again
        wall_manager.adjust_status(
            wall=w1,
            adjusted_status=WallAdjustedStatus.CPI_ADJUSTMENT,
            adjusted_cpi=Decimal("0.50"),
            adjusted_timestamp=adj_ts,
        )
        assert WallAdjustedStatus.CPI_ADJUSTMENT == w1.adjusted_status
        assert Decimal("0.50") == w1.adjusted_cpi
        new_status, new_payout, new_user_payout = s1.determine_new_status_and_payouts()
        assert Status.COMPLETE == new_status
        assert Decimal("0.48") == new_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal("0.24") == new_user_payout
        assert new_user_payout is None

        s1.adjust_status()
        assert SessionAdjustedStatus.PAYOUT_ADJUSTMENT == s1.adjusted_status
        assert Decimal("0.48") == s1.adjusted_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal("0.24") == s1.adjusted_user_payout
        assert s1.adjusted_user_payout is None

    def test_complete_to_fail_to_complete(self, user, session_factory, utc_hour_ago):
        # Setup: Complete, then adjust it to fail
        s1 = session_factory(
            user=user,
            wall_count=1,
            wall_req_cpi=Decimal("1"),
            final_status=Status.COMPLETE,
            started=utc_hour_ago,
        )
        w1 = s1.wall_events[0]

        status, status_code_1 = s1.determine_session_status()
        thl_net, commission_amount, bp_pay, user_pay = s1.determine_payments()
        s1.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": utc_hour_ago + timedelta(minutes=10),
                "payout": bp_pay,
                "user_payout": user_pay,
            }
        )
        w1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=adj_ts,
        )
        s1.adjust_status()

        # Test: Adjust back to complete
        w1.update(
            adjusted_status=None,
            adjusted_cpi=None,
            adjusted_timestamp=adj_ts,
        )
        assert w1.adjusted_status is None
        assert w1.adjusted_cpi is None
        assert adj_ts == w1.adjusted_timestamp

        new_status, new_payout, new_user_payout = s1.determine_new_status_and_payouts()
        assert Status.COMPLETE == new_status
        assert Decimal("0.95") == new_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal("0.48") == new_user_payout
        assert new_user_payout is None

        s1.adjust_status()
        assert s1.adjusted_status is None
        assert s1.adjusted_payout is None
        assert s1.adjusted_user_payout is None

    def test_complete_to_fail_to_complete_adj(
        self, user, session_factory, utc_hour_ago
    ):
        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpis=[Decimal(1), Decimal(2)],
            final_status=Status.COMPLETE,
            started=utc_hour_ago,
        )

        w1 = s1.wall_events[0]
        w2 = s1.wall_events[1]

        status, status_code_1 = s1.determine_session_status()
        thl_net = Decimal(sum(w.cpi for w in s1.wall_events if w.is_visible_complete()))
        payout = user.product.determine_bp_payment(thl_net=thl_net)
        s1.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": utc_hour_ago + timedelta(minutes=25),
                "payout": payout,
                "user_payout": None,
            }
        )

        # Test. Adjust first fail to complete. Now we have 2 completes.
        w1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w1.cpi,
            adjusted_timestamp=adj_ts,
        )
        s1.adjust_status()
        assert SessionAdjustedStatus.PAYOUT_ADJUSTMENT == s1.adjusted_status
        assert Decimal("2.85") == s1.adjusted_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal("1.42") == s1.adjusted_user_payout
        assert s1.adjusted_user_payout is None

        # Now we have [Fail, Complete ($2)] -> [Complete ($1), Fail]
        w2.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=adj_ts2,
        )
        s1.adjust_status()
        assert SessionAdjustedStatus.PAYOUT_ADJUSTMENT == s1.adjusted_status
        assert Decimal("0.95") == s1.adjusted_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal("0.48") == s1.adjusted_user_payout
        assert s1.adjusted_user_payout is None

    def test_complete_to_fail_to_complete_adj1(
        self, user, session_factory, utc_hour_ago
    ):
        # Same as test_complete_to_fail_to_complete_adj but in opposite order
        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpis=[Decimal(1), Decimal(2)],
            final_status=Status.COMPLETE,
            started=utc_hour_ago,
        )

        w1 = s1.wall_events[0]
        w2 = s1.wall_events[1]

        status, status_code_1 = s1.determine_session_status()
        thl_net = Decimal(sum(w.cpi for w in s1.wall_events if w.is_visible_complete()))
        payout = user.product.determine_bp_payment(thl_net)
        s1.update(
            **{
                "status": status,
                "status_code_1": status_code_1,
                "finished": utc_hour_ago + timedelta(minutes=25),
                "payout": payout,
                "user_payout": None,
            }
        )

        # Test. Adjust complete to fail. Now we have 2 fails.
        w2.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_FAIL,
            adjusted_cpi=0,
            adjusted_timestamp=adj_ts,
        )
        s1.adjust_status()
        assert SessionAdjustedStatus.ADJUSTED_TO_FAIL == s1.adjusted_status
        assert Decimal(0) == s1.adjusted_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal(0) == s.adjusted_user_payout
        assert s1.adjusted_user_payout is None
        # Now we have [Fail, Complete ($2)] -> [Complete ($1), Fail]
        w1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w1.cpi,
            adjusted_timestamp=adj_ts2,
        )
        s1.adjust_status()
        assert SessionAdjustedStatus.PAYOUT_ADJUSTMENT == s1.adjusted_status
        assert Decimal("0.95") == s1.adjusted_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal("0.48") == s.adjusted_user_payout
        assert s1.adjusted_user_payout is None

    def test_fail_to_complete_to_fail(self, user, session_factory, utc_hour_ago):
        # End with an abandon
        s1 = session_factory(
            user=user,
            wall_count=2,
            wall_req_cpis=[Decimal(1), Decimal(2)],
            final_status=Status.ABANDON,
            started=utc_hour_ago,
        )

        w1 = s1.wall_events[0]
        w2 = s1.wall_events[1]

        # abandon adjust to complete
        w2.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w2.cpi,
            adjusted_timestamp=adj_ts,
        )
        assert WallAdjustedStatus.ADJUSTED_TO_COMPLETE == w2.adjusted_status
        s1.adjust_status()
        assert SessionAdjustedStatus.ADJUSTED_TO_COMPLETE == s1.adjusted_status
        assert Decimal("1.90") == s1.adjusted_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal("0.95") == s1.adjusted_user_payout
        assert s1.adjusted_user_payout is None

        # back to fail
        w2.update(
            adjusted_status=None,
            adjusted_cpi=None,
            adjusted_timestamp=adj_ts,
        )
        assert w2.adjusted_status is None
        s1.adjust_status()
        assert s1.adjusted_status is None
        assert s1.adjusted_payout is None
        assert s1.adjusted_user_payout is None

        # other is now complete
        w1.update(
            adjusted_status=WallAdjustedStatus.ADJUSTED_TO_COMPLETE,
            adjusted_cpi=w1.cpi,
            adjusted_timestamp=adj_ts,
        )
        assert WallAdjustedStatus.ADJUSTED_TO_COMPLETE == w1.adjusted_status
        s1.adjust_status()
        assert SessionAdjustedStatus.ADJUSTED_TO_COMPLETE == s1.adjusted_status
        assert Decimal("0.95") == s1.adjusted_payout
        assert not user.product.user_wallet_config.enabled
        # assert Decimal("0.48") == s1.adjusted_user_payout
        assert s1.adjusted_user_payout is None
