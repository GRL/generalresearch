from datetime import timezone, datetime
from typing import List, Optional, Literal, cast, Collection, Tuple, Dict
from uuid import UUID

import redis
from pydantic import PositiveInt, NonNegativeInt
from redis import Redis

from generalresearch.managers.base import PostgresManager
from generalresearch.managers.thl.ledger_manager.thl_ledger import (
    ThlLedgerManager,
)
from generalresearch.managers.thl.user_manager.user_manager import (
    UserManager,
)
from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.contest import (
    ContestWinner,
    ContestPrize,
)
from generalresearch.models.thl.contest.contest import (
    Contest,
    ContestUserView,
)
from generalresearch.models.thl.contest.definitions import (
    ContestStatus,
    ContestType,
)
from generalresearch.models.thl.contest.exceptions import ContestError
from generalresearch.models.thl.contest.io import (
    ContestCreate,
    contest_create_to_contest,
    model_cls,
    user_model_cls,
)
from generalresearch.models.thl.contest.leaderboard import (
    LeaderboardContestUserView,
    LeaderboardContest,
)
from generalresearch.models.thl.contest.milestone import (
    MilestoneUserView,
    MilestoneEntry,
    ContestEntryTrigger,
    MilestoneContest,
)
from generalresearch.models.thl.contest.raffle import (
    ContestEntry,
    ContestEntryType,
    RaffleUserView,
    RaffleContest,
)
from generalresearch.models.thl.user import User

CONTEST_SELECT = """
    c.id,
    c.uuid::uuid,
    c.product_id::uuid,
    c.name,
    c.description,
    c.country_isos,
    c.contest_type,
    c.status,
    c.starts_at::timestamptz,
    c.terms_and_conditions,
    c.end_condition::jsonb,
    c.prizes::jsonb,
    c.ended_at::timestamptz,
    c.end_reason,
    c.entry_type,
    c.entry_rule::jsonb,
    c.current_participants,
    c.current_amount,
    c.milestone_config::jsonb,
    c.win_count,
    c.leaderboard_key,
    c.created_at::timestamptz,
    c.updated_at::timestamptz"""

USER_SELECT = """
    u.id as user_id,
    u.uuid::uuid as user_uuid,
    u.product_id::uuid,
    u.product_user_id"""

USER_WINNINGS_JOIN = """        
LEFT JOIN LATERAL (
SELECT
    jsonb_agg(
        jsonb_build_object(
            'uuid', cw.uuid::uuid,
            'prize', cw.prize::jsonb,
            'created_at', cw.created_at::timestamptz
        )
    ) AS user_winnings,
    MAX(cw.created_at) AS last_won
FROM contest_contestwinner cw
WHERE cw.contest_id = c.id
  AND cw.user_id = %(user_id)s
) cw_json ON TRUE"""

USER_ENTRIES_JOIN = """
LEFT JOIN LATERAL (
    SELECT
        COALESCE(SUM(ce.amount), 0) AS user_amount,
        COALESCE(
            SUM(
                CASE WHEN ce.created_at > NOW() - INTERVAL '24 hours'
                     THEN ce.amount ELSE 0 END
            ), 0
        ) AS user_amount_today,
        MAX(ce.created_at)::timestamptz AS entry_last_created
    FROM contest_contestentry ce
    WHERE ce.contest_id = c.id
      AND ce.user_id = %(user_id)s
) ce_agg ON TRUE
"""


class ContestBaseManager(PostgresManager):

    def create(self, product_id: UUIDStr, contest_create: ContestCreate) -> Contest:
        contest = contest_create_to_contest(
            product_id=product_id, contest_create=contest_create
        )
        data = contest.model_dump_mysql()
        fields = set(data.keys())

        fields_str = ", ".join(fields)
        values_str = ", ".join([f"%({x})s" for x in fields])
        query = f"""
        INSERT INTO contest_contest ({fields_str}) 
        VALUES ({values_str})
        RETURNING id;
        """

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=query,
                    params=data,
                )
                pk = c.fetchone()["id"]
            conn.commit()

        contest.id = pk
        return contest

    def get(self, contest_uuid: UUIDStr) -> Contest:
        contest_uuid = UUID(contest_uuid).hex
        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT {CONTEST_SELECT}
                FROM contest_contest c
                WHERE c.uuid = %(contest_uuid)s
                LIMIT 2;
            """,
            params={"contest_uuid": contest_uuid},
        )
        # uuid column has a unique constraint. there can't possibly be >1
        if len(res) == 0:
            raise ValueError("Contest not found")

        d = res[0]
        return model_cls[d["contest_type"]].model_validate_mysql(d)

    def get_if_exists(self, contest_uuid: UUIDStr) -> Optional[Contest]:
        try:
            return self.get(contest_uuid=contest_uuid)

        except ValueError as e:
            if e.args[0] == "Contest not found":
                return None
            raise e

    @staticmethod
    def make_filter_str(
        product_id: Optional[str] = None,
        status: Optional[ContestStatus] = None,
        contest_type: Optional[ContestType] = None,
        starts_at_before: Optional[datetime | bool] = None,
        name: Optional[str] = None,
        name_contains: Optional[str] = None,
        uuids: Optional[Collection[str]] = None,
        has_participants: Optional[bool] = None,
    ) -> Tuple[str, Dict]:
        filters = []
        params = dict()
        if product_id:
            params["product_id"] = product_id
            filters.append("product_id = %(product_id)s")
        if status:
            params["status"] = status.value
            filters.append("status = %(status)s")
        if contest_type:
            params["contest_type"] = contest_type.value
            filters.append("contest_type = %(contest_type)s")
        if starts_at_before is True:
            params["starts_at"] = datetime.now(tz=timezone.utc)
            filters.append("starts_at < %(starts_at)s")
        elif starts_at_before:
            assert starts_at_before.tzinfo == timezone.utc
            params["starts_at"] = starts_at_before
            filters.append("starts_at < %(starts_at)s")
        if name is not None:
            params["name"] = name
            filters.append("name = %(name)s")
        if name_contains is not None:
            params["name_contains"] = f"%{name_contains}%"
            filters.append("name ILIKE %(name_contains)s")
        if uuids is not None:
            if len(uuids) == 0:
                # If we pass an empty list, the sql query will have a syntax error. Make it
                #   instead a legal filter, that will return nothing.
                uuids = ["0" * 32]
            params["uuids"] = uuids
            filters.append("uuid = ANY(%(uuids)s)")
        if has_participants:
            filters.append("current_participants > 0")

        filter_str = "WHERE " + " AND ".join(filters) if filters else ""
        return filter_str, params

    def get_many(
        self,
        product_id: Optional[str] = None,
        status: Optional[ContestStatus] = None,
        contest_type: Optional[ContestType] = None,
        starts_at_before: Optional[datetime | bool] = None,
        name: Optional[str] = None,
        name_contains: Optional[str] = None,
        uuids: Optional[Collection[str]] = None,
        has_participants: Optional[bool] = None,
        page: Optional[int] = None,
        size: Optional[int] = None,
        include_winners: bool = True,
    ) -> List[Contest]:

        filter_str, params = self.make_filter_str(
            product_id=product_id,
            status=status,
            contest_type=contest_type,
            starts_at_before=starts_at_before,
            name=name,
            name_contains=name_contains,
            uuids=uuids,
            has_participants=has_participants,
        )

        paginated_filter_str = ""
        if page is not None:
            assert page != 0, "page starts at 1"
            size = size if size is not None else 100
            params["offset"] = (page - 1) * size
            params["limit"] = size
            paginated_filter_str = " LIMIT %(limit)s OFFSET %(offset)s"

        # set "order by" as a param? Would like "ending soonest", but that is not easy to query
        order_by_str = "ORDER BY created_at DESC"

        if include_winners:
            query = f"""
                SELECT {CONTEST_SELECT},
                    COALESCE(cw_json.all_winners, '[]'::jsonb) AS all_winners
                FROM contest_contest c
                LEFT JOIN (
                     SELECT
                        cw.contest_id,
                        jsonb_agg(
                            jsonb_build_object(
                                'uuid', cw.uuid,
                                'prize', cw.prize,
                                'created_at', cw.created_at,
                                'user_id', cw.user_id,
                                'user_uuid', u.uuid::uuid,
                                'product_id', u.product_id::uuid,
                                'product_user_id', u.product_user_id
                            )
                        ) AS all_winners
                    FROM contest_contestwinner cw
                    JOIN thl_user u ON u.id = cw.user_id
                    GROUP BY cw.contest_id
                ) AS cw_json ON cw_json.contest_id = c.id
                {filter_str}
                {order_by_str} {paginated_filter_str} ;
            """

        else:
            query = f"""
                SELECT {CONTEST_SELECT}
                FROM contest_contest c
                {filter_str}
                {order_by_str} {paginated_filter_str} ;
            """

        # print(query)
        sql_res = self.pg_config.execute_sql_query(query=query, params=params)
        res = []
        for d in sql_res:
            if include_winners:
                for x in d["all_winners"]:
                    x["uuid"] = UUID(x["uuid"]).hex
                    x["created_at"] = datetime.fromisoformat(x["created_at"])
                    x["user"] = self.parse_user_from_row(x)
            c: Contest = model_cls[d["contest_type"]].model_validate_mysql(d)
            res.append(c)
        return res

    def get_many_by_user_eligible_raffle(
        self, user: User, country_iso: str
    ) -> List[RaffleUserView]:
        # Seems like this is a known pycharm bug. Doing it this way to be explicit.
        # https://youtrack.jetbrains.com/issue/PY-42473/Type-inference-broken-for-Literal-with-Enum
        cs = self.get_many_by_user_eligible(
            user=user, country_iso=country_iso, contest_type=ContestType.RAFFLE
        )
        return cast(List[RaffleUserView], cs)

    def get_many_by_user_eligible_milestone(
        self,
        user: User,
        country_iso: str,
        entry_trigger: Optional[ContestEntryTrigger] = None,
    ) -> List[MilestoneUserView]:
        cs = self.get_many_by_user_eligible(
            user=user,
            country_iso=country_iso,
            contest_type=ContestType.MILESTONE,
            entry_trigger=entry_trigger,
        )
        return cast(List[MilestoneUserView], cs)

    def get_many_by_user_eligible(
        self,
        user: User,
        country_iso: str,
        contest_type: Optional[ContestType] = None,
        entry_trigger: Optional[ContestEntryTrigger] = None,
    ) -> List[ContestUserView]:
        # Get by product_id, and status OPEN. Then we have to filter in python.
        #   (could also add country filter into mysql)
        assert user.user_id, "invalid user"
        assert user.product_id, "invalid user"

        if entry_trigger:
            assert contest_type == ContestType.MILESTONE

        params = {"user_id": user.user_id, "product_id": user.product_id}
        filters = []
        if contest_type:
            params["contest_type"] = contest_type.value
            filters.append("contest_type = %(contest_type)s")
        if entry_trigger:
            params["entry_trigger"] = entry_trigger.value
            filters.append(
                "milestone_config::jsonb->>'entry_trigger' = %(entry_trigger)s"
            )
        filter_str = " AND " + " AND ".join(filters) if filters else ""
        sql_res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT 
                    {CONTEST_SELECT},
                    ce_agg.user_amount,
                    ce_agg.user_amount_today
                FROM contest_contest c
                {USER_ENTRIES_JOIN}
                WHERE product_id = %(product_id)s AND status = 'active' 
                {filter_str};
            """,
            params=params,
        )

        res = []
        for d in sql_res:
            d["product_user_id"] = user.product_user_id
            c: ContestUserView = user_model_cls[d["contest_type"]].model_validate_mysql(
                d
            )
            passes, _ = c.is_user_eligible(country_iso=country_iso)
            if passes:
                res.append(c)

        return res

    def get_many_by_user_entered(
        self,
        user: User,
        limit: Optional[PositiveInt] = 100,
        order_by: Literal["recent_enter", "ending_soon"] = "recent_enter",
    ) -> List[ContestUserView]:
        """
        This sets the user_contest_info field as well, which calculates the
        user's entry count and win percentages.
        We need: user_amount, and user_winnings
        """
        assert user.user_id, "invalid user"
        params = {"user_id": user.user_id}

        if order_by == "recent_enter":
            order_by_str = "ORDER BY entry_last_created DESC"
        else:
            # don't really have a good way of doing this yet ... lol. Sort
            # by oldest contest instead
            order_by_str = "ORDER BY c.created_at ASC"

        query = f"""
            SELECT 
                {CONTEST_SELECT},
                ce_agg.user_amount,
                ce_agg.user_amount_today,
                ce_agg.entry_last_created,
                COALESCE(cw_json.user_winnings, '[]'::jsonb) AS user_winnings,
                {USER_SELECT}
            FROM contest_contest c
            JOIN thl_user u 
                ON u.id = %(user_id)s
            JOIN contest_contestentry ce0
                ON ce0.contest_id = c.id
                AND ce0.user_id = %(user_id)s
            {USER_ENTRIES_JOIN}
            {USER_WINNINGS_JOIN}
            {order_by_str}
            LIMIT {limit}
        """
        sql_res = self.pg_config.execute_sql_query(
            query=query,
            params=params,
        )

        res = []
        for d in sql_res:
            for x in d["user_winnings"]:
                x["uuid"] = UUID(x["uuid"]).hex
                x["created_at"] = datetime.fromisoformat(x["created_at"])
                x["user"] = user
            c: ContestUserView = user_model_cls[d["contest_type"]].model_validate_mysql(
                d
            )
            res.append(c)

        return res

    def get_many_by_user_won(
        self,
        user: User,
        limit: Optional[PositiveInt] = 100,
    ) -> List[ContestUserView]:
        """
        This sets the user_contest_info field as well, which calculates the
        user's entry count and win percentages.
        """
        assert user.user_id, "invalid user"
        params = {"user_id": user.user_id}
        query = f"""
            SELECT 
                {CONTEST_SELECT},
                ce_agg.user_amount,
                ce_agg.user_amount_today,
                COALESCE(cw_json.user_winnings, '[]'::jsonb) AS user_winnings,
                cw_json.last_won AS contest_last_won,
                {USER_SELECT}
            FROM contest_contest c
            JOIN thl_user u 
                ON u.id = %(user_id)s
            {USER_ENTRIES_JOIN}
            {USER_WINNINGS_JOIN}
            WHERE EXISTS (
                SELECT 1
                FROM contest_contestwinner w
                WHERE w.contest_id = c.id
                  AND w.user_id = %(user_id)s
            )
            ORDER BY contest_last_won DESC
            LIMIT {limit}
        """
        sql_res = self.pg_config.execute_sql_query(
            query=query,
            params=params,
        )
        res = []
        for d in sql_res:
            for x in d["user_winnings"]:
                x["uuid"] = UUID(x["uuid"]).hex
                x["created_at"] = datetime.fromisoformat(x["created_at"])
                x["user"] = user
            c: ContestUserView = user_model_cls[d["contest_type"]].model_validate_mysql(
                d
            )
            res.append(c)

        return res

    @staticmethod
    def parse_user_from_row(d: Dict):
        return User(
            uuid=UUID(d["user_uuid"]).hex,
            user_id=d["user_id"],
            product_user_id=d["product_user_id"],
            product_id=UUID(d["product_id"]).hex,
        )

    def get_winnings_by_user(self, user: User) -> List[ContestWinner]:
        assert user.user_id, "invalid user"
        sql_res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT 
                    cw.id,
                    cw.uuid::uuid, 
                    cw.contest_id,
                    cw.prize::jsonb,
                    cw.awarded_cash_amount,
                    cw.created_at::timestamptz,
                    {USER_SELECT}
                FROM contest_contestwinner cw
                JOIN thl_user u 
                    ON u.id = cw.user_id
                WHERE user_id = %(user_id)s
            """,
            params={"user_id": user.user_id},
        )

        res = []
        for x in sql_res:
            x["uuid"] = UUID(x["uuid"]).hex
            x["prize"] = ContestPrize.model_validate(x["prize"])
            x["user"] = user
            res.append(ContestWinner.model_validate(x))

        return res

    def get_entries_by_contest_id(self, contest_id: PositiveInt) -> List[ContestEntry]:

        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT
                    ce.id,
                    ce.uuid::uuid,
                    ce.contest_id,
                    ce.amount,
                    ce.user_id,
                    ce.created_at::timestamptz,
                    ce.updated_at::timestamptz, 
                    c.entry_type,
                    {USER_SELECT}
                FROM contest_contestentry ce
                JOIN contest_contest c 
                    ON c.id = ce.contest_id
                JOIN thl_user u 
                    ON u.id = ce.user_id
                WHERE ce.contest_id = %(contest_id)s
            """,
            params={"contest_id": contest_id},
        )
        for x in res:
            x["user"] = self.parse_user_from_row(x)
        return [ContestEntry.model_validate(x) for x in res]

    def end_contest_with_winners(
        self, contest: Contest, ledger_manager: ThlLedgerManager
    ) -> None:
        assert contest.status == ContestStatus.COMPLETED, "status must be completed"
        data = {
            "status": contest.status.value,
            "ended_at": contest.ended_at,
            "end_reason": contest.end_reason,
            "contest_uuid": contest.uuid,
        }
        winners = contest.all_winners

        assert contest.id
        rows = [w.model_dump_mysql(contest_id=contest.id) for w in winners]

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(
                    query="""
                        INSERT INTO contest_contestwinner
                                    (uuid, created_at, user_id, 
                                    contest_id, prize, awarded_cash_amount) 
                            VALUES  (%(uuid)s, %(created_at)s, %(user_id)s, 
                                    %(contest_id)s, %(prize)s, %(awarded_cash_amount)s)
                    """,
                    params_seq=rows,
                )
                c.execute(
                    query="""
                        UPDATE contest_contest
                        SET status = %(status)s, 
                            ended_at = %(ended_at)s, 
                            end_reason = %(end_reason)s
                        WHERE uuid = %(contest_uuid)s
                        AND status = 'active'
                    """,
                    params=data,
                )
                assert c.rowcount == 1, "Contest changed during write"
            conn.commit()
        ledger_manager.create_tx_contest_close(contest=contest)
        return None

    def cancel_contest(self, contest: Contest) -> int:
        assert contest.status == ContestStatus.CANCELLED, "status must be cancelled"

        return self.pg_config.execute_write(
            query="""
                UPDATE contest_contest
                SET status = %(status)s
                WHERE uuid = %(contest_uuid)s
            """,
            params={
                "contest_uuid": contest.uuid,
                "status": contest.status,
            },
        )


class RaffleContestManager(ContestBaseManager):

    def get_raffle_user_view(self, contest_uuid: UUIDStr, user: User) -> RaffleUserView:

        assert user.user_id and user.product_user_id, "invalid user"
        query = f"""
        SELECT
            {CONTEST_SELECT},
            ce_agg.user_amount,
            ce_agg.user_amount_today,
            COALESCE(cw_json.user_winnings, '[]'::jsonb) AS user_winnings,
            {USER_SELECT}
        FROM contest_contest c
        JOIN thl_user u ON u.id = %(user_id)s
        {USER_ENTRIES_JOIN}
        {USER_WINNINGS_JOIN}
        WHERE c.uuid = %(contest_uuid)s;
        """
        sql_res = self.pg_config.execute_sql_query(
            query=query,
            params={"user_id": user.user_id, "contest_uuid": contest_uuid},
        )
        assert len(sql_res) == 1
        d = sql_res[0]
        for x in d["user_winnings"]:
            x["uuid"] = UUID(x["uuid"]).hex
            x["created_at"] = datetime.fromisoformat(x["created_at"])
            x["user"] = user
        return RaffleUserView.model_validate_mysql(d)

    def enter_contest(
        self,
        contest_uuid: UUIDStr,
        entry: ContestEntry,
        country_iso: str,
        ledger_manager: ThlLedgerManager,
    ) -> ContestEntry:
        """
        - Validates user is eligible to enter this contest
        We need to look up the contest, b/c we need the contest-user-view,
            with counts n stuff scoped to the requesting user
        - If it is a cash contest:
          - validates user has balance in their wallet
          - does ledger txs, does enter_contest_db()
        - else:
          - enter_contest_db()
        Note: for milestone contests, the API should prevent a user from
            trying to enter it
        """
        contest = self.get_raffle_user_view(contest_uuid=contest_uuid, user=entry.user)
        assert contest.contest_type == ContestType.RAFFLE, "can only enter a raffle"
        assert isinstance(contest, RaffleUserView)
        assert contest.entry_type == entry.entry_type, "incompatible entry type"

        res, msg = contest.is_entry_eligible(entry=entry)
        if not res:
            raise ContestError(msg)

        res, msg = contest.is_user_eligible(country_iso=country_iso)
        if not res:
            raise ContestError(msg)

        if contest.entry_type == ContestEntryType.CASH:
            tx = ledger_manager.create_tx_user_enter_contest(
                contest_uuid=contest.uuid, contest_entry=entry
            )

        entry = self.enter_contest_db_work_raffle(contest=contest, entry=entry)
        decision, msg = contest.should_end()
        if decision:
            contest.end_contest()
            self.end_contest_with_winners(contest, ledger_manager)

        return entry

    def enter_contest_db_work_raffle(
        self, contest: RaffleContest, entry: ContestEntry
    ) -> ContestEntry:
        assert contest.id, "Contest must be saved."

        # todo: retry if this fails
        # 1) get the contest with all its entries,
        contest.entries = self.get_entries_by_contest_id(contest_id=contest.id)
        assert contest.current_amount == contest.get_current_amount()
        assert contest.current_participants == contest.get_current_participants()
        old_current_amount = contest.current_amount

        # 2) calculate new values of current_participants and current_amount
        contest.entries.append(entry)
        contest.current_amount = contest.get_current_amount()
        contest.current_participants = contest.get_current_participants()

        data = entry.model_dump_mysql(contest_id=contest.id)

        # 3) IN 1 DB TX: update these 2 field on the contest, and create the entry
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query=f"""
                        INSERT INTO contest_contestentry
                                (uuid, amount, user_id, 
                                created_at, updated_at, contest_id) 
                        VALUES  (%(uuid)s, %(amount)s, %(user_id)s, 
                                %(created_at)s, %(updated_at)s, %(contest_id)s)
                    """,
                    params=data,
                )
                c.execute(
                    query=f"""
                        UPDATE contest_contest
                        SET current_amount = %(current_amount)s, 
                            current_participants = %(current_participants)s
                        WHERE id = %(contest_id)s
                        -- Double click / Lock protection. No rows will be 
                        -- changed if someone tries to enter the contest 
                        -- while we're in the middle of this transaction
                        AND current_amount = %(old_current_amount)s
                    """,
                    params={
                        "old_current_amount": old_current_amount,
                        "current_amount": contest.current_amount,
                        "current_participants": contest.current_participants,
                        "contest_id": contest.id,
                    },
                )
                assert (
                    c.rowcount == 1
                ), "enter_contest_db_work_raffle: Mismatch amounts in contest entry"
            conn.commit()

        return entry


class MilestoneContestManager(ContestBaseManager):
    def get_milestone_user_view(
        self, contest_uuid: UUIDStr, user: User
    ) -> MilestoneUserView:

        assert user.user_id and user.product_user_id, "invalid user"

        # Note: do NOT just join both tables, or you'll end up with "JOIN multiplication".
        #   Have to join the contestwinner in a subquery.
        # Note: In a milestone contest, there will only be 0 or 1 contest_contestentry rows
        #   per (user, contest), so no aggregation is done (for user_amount, etc).
        sql_res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT 
                    {CONTEST_SELECT},
                    COALESCE(ce.amount, 0) AS user_amount,
                    COALESCE(cw_json.user_winnings, '[]'::jsonb) AS user_winnings,
                    {USER_SELECT}
                FROM contest_contest c
                JOIN thl_user u 
                    ON u.id = %(user_id)s
                LEFT JOIN contest_contestentry ce 
                    ON ce.contest_id = c.id 
                    AND ce.user_id = %(user_id)s
                {USER_WINNINGS_JOIN}
                WHERE c.uuid = %(contest_uuid)s
                LIMIT 2;
            """,
            params={"user_id": user.user_id, "contest_uuid": contest_uuid},
        )
        assert len(sql_res) == 1

        d = sql_res[0]
        for x in d["user_winnings"]:
            x["uuid"] = UUID(x["uuid"]).hex
            x["created_at"] = datetime.fromisoformat(x["created_at"])
            x["user"] = user

        return MilestoneUserView.model_validate_mysql(d)

    def enter_milestone_contest(
        self,
        contest_uuid: UUIDStr,
        user: User,
        country_iso: str,
        ledger_manager: ThlLedgerManager,
        incr: PositiveInt = 1,
    ) -> None:
        """
        This is "enter_contest" but for a milestone contest. There is a single
        contest entry record per (contest, user). We'll validate the user is
        eligible, then create or update it, then do contest maintenance.
        """
        contest = self.get_milestone_user_view(contest_uuid=contest_uuid, user=user)
        assert (
            contest.contest_type == ContestType.MILESTONE
        ), "can only enter a milestone"
        assert isinstance(contest, MilestoneUserView)

        res, msg = contest.is_user_eligible(country_iso=country_iso)
        if not res:
            raise ContestError(msg)

        self.enter_contest_db_work_milestone(contest=contest, user=user, incr=incr)
        if contest.should_award():
            self.award_milestone_contest(contest, user, ledger_manager=ledger_manager)

        decision, reason = contest.should_end()
        if decision:
            contest.update(
                status=ContestStatus.COMPLETED,
                ended_at=datetime.now(tz=timezone.utc),
                end_reason=reason,
            )
            self.end_milestone_contest(contest)

        return None

    def enter_contest_db_work_milestone(
        self, contest: MilestoneUserView, user: User, incr: PositiveInt
    ) -> MilestoneEntry:
        # Single entry per entry, sum to user's previous if exists
        entry = MilestoneEntry(user=user, amount=incr)
        data = entry.model_dump_mysql(contest_id=contest.id)
        if contest.user_amount == 0:
            self.pg_config.execute_write(
                query="""
                    INSERT INTO contest_contestentry
                                (uuid, amount, user_id, 
                                created_at, updated_at, contest_id) 
                        VALUES  (%(uuid)s, %(amount)s, %(user_id)s, 
                                %(created_at)s, %(updated_at)s, %(contest_id)s)
                """,
                params=data,
            )
            contest.user_amount = entry.amount
        else:
            with self.pg_config.make_connection() as conn:
                with conn.cursor() as c:
                    c.execute(
                        query="""
                            UPDATE contest_contestentry
                            SET amount = amount + %(amount)s, 
                                updated_at = %(updated_at)s
                            WHERE user_id = %(user_id)s 
                                AND contest_id = %(contest_id)s
                                AND amount = %(current_amount)s
                        """,
                        params=data | {"current_amount": contest.user_amount},
                    )
                    assert (
                        c.rowcount == 1
                    ), "enter_contest_db_work_milestone: Mismatch amounts in contest entry"
                conn.commit()
            contest.user_amount += entry.amount
        return entry

    def end_milestone_contest(self, contest: MilestoneContest) -> None:
        """
        A milestone contest has (possibly) paid out user's award already (once
        each user has reached the milestone). So when the contest itself is
        over, nothing really happens, money-wise.
        """
        assert contest.status == ContestStatus.COMPLETED, "status must be completed"
        assert isinstance(contest, MilestoneContest), "must pass MilestoneContest"
        data = {
            "status": contest.status.value,
            "ended_at": contest.ended_at,
            "end_reason": contest.end_reason,
            "contest_uuid": contest.uuid,
        }
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    query="""
                        UPDATE contest_contest
                        SET status = %(status)s, 
                            ended_at = %(ended_at)s, 
                            end_reason = %(end_reason)s
                        WHERE uuid = %(contest_uuid)s
                        AND status = 'active'
                    """,
                    params=data,
                )
                assert c.rowcount == 1, "Contest changed during write"
            conn.commit()
        return None

    def award_milestone_contest(
        self,
        contest: MilestoneUserView,
        user: User,
        ledger_manager: ThlLedgerManager,
    ) -> None:
        """A user reached the milestone. The contest stays open (unless it
        has reached the max winners).
        """
        assert contest.should_award()
        assert not contest.user_winnings, "user already was awarded"
        winners = [ContestWinner(prize=prize, user=user) for prize in contest.prizes]
        rows = [w.model_dump_mysql(contest_id=contest.id) for w in winners]
        # The win_count is 1 !!! A user can only "win" a milestone once, no matter how
        #   many prizes are awarded.
        win_count = 1

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.executemany(
                    query="""
                        INSERT INTO contest_contestwinner
                                (uuid, created_at, user_id, 
                                contest_id, prize, awarded_cash_amount) 
                        VALUES  (%(uuid)s, %(created_at)s, %(user_id)s, 
                                %(contest_id)s, %(prize)s, %(awarded_cash_amount)s)
                """,
                    params_seq=rows,
                )
                c.execute(
                    query="""
                        UPDATE contest_contest
                        SET win_count = win_count + %(win_count)s
                        WHERE uuid = %(contest_uuid)s
                    """,
                    params={
                        "contest_uuid": contest.uuid,
                        "win_count": win_count,
                    },
                )
            conn.commit()
        contest.win_count += win_count
        ledger_manager.create_tx_milestone_winner(contest=contest, winners=winners)
        return None


class LeaderboardContestManager(ContestBaseManager):
    def get_leaderboard_user_view(
        self,
        contest_uuid: UUIDStr,
        user: User,
        redis_client: redis.Redis,
        user_manager: UserManager,
    ) -> LeaderboardContestUserView:
        """
        A leaderboard contest has NO user_entries. The redis leaderboard
        manager handles tracking everything.
        """
        assert user.user_id and user.product_user_id, "invalid user"

        sql_res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT 
                    {CONTEST_SELECT},
                    COALESCE(cw_json.user_winnings, '[]'::jsonb) AS user_winnings,
                    {USER_SELECT}
                FROM contest_contest c
                JOIN thl_user u 
                    ON u.id = %(user_id)s
                {USER_WINNINGS_JOIN}
                WHERE c.uuid = %(contest_uuid)s
                LIMIT 2;
            """,
            params={"user_id": user.user_id, "contest_uuid": contest_uuid},
        )
        assert len(sql_res) == 1

        d = sql_res[0]
        for x in d["user_winnings"]:
            x["uuid"] = UUID(x["uuid"]).hex
            x["created_at"] = datetime.fromisoformat(x["created_at"])
            x["user"] = user
        c = LeaderboardContestUserView.model_validate_mysql(d)
        c._redis_client = redis_client
        c._user_manager = user_manager

        return c

    def end_contest_if_over(
        self, contest: Contest, ledger_manager: ThlLedgerManager
    ) -> None:
        decision, reason = contest.should_end()
        if decision:
            contest.end_contest()
            return self.end_contest_with_winners(contest, ledger_manager)


class ContestManager(
    RaffleContestManager, MilestoneContestManager, LeaderboardContestManager
):

    def end_contest(self, contest: Contest, ledger_manager: ThlLedgerManager) -> None:
        if isinstance(contest, (MilestoneContest)):
            return self.end_milestone_contest(contest)

        elif isinstance(contest, (LeaderboardContest, RaffleContest)):
            return self.end_contest_with_winners(contest, ledger_manager=ledger_manager)

    def check_for_contest_closing(
        self,
        ledger_manager: ThlLedgerManager,
        redis_client: Redis,
        user_manager: UserManager,
    ) -> Dict[str, NonNegativeInt]:
        # This is an administrative function that we'll run on a schedule,
        # that will check for any open contests, for any BP, that should be
        # closed, and then do it!
        page = 1
        contests_checked = 0
        contests_closed = 0
        while True:
            contests = self.get_many(
                status=ContestStatus.ACTIVE,
                include_winners=False,
                starts_at_before=True,
                page=page,
                size=20,
            )
            if not contests:
                break
            print(f"Got {len(contests)} contests")
            contests_checked += len(contests)
            this_contests_closed = self.check_for_contest_closing_chunk(
                contests,
                ledger_manager=ledger_manager,
                redis_client=redis_client,
                user_manager=user_manager,
            )
            contests_closed += this_contests_closed
            print(f"Closed {this_contests_closed} contests")
            page += 1

        return {"closed": contests_closed, "checked": contests_checked}

    def check_for_contest_closing_chunk(
        self,
        contests: Collection[Contest],
        ledger_manager: ThlLedgerManager,
        redis_client: Redis,
        user_manager: UserManager,
    ) -> NonNegativeInt:
        contests_closed = 0
        for contest in contests:
            should_end, reason = contest.should_end()
            if should_end:
                if hasattr(contest, "redis_client"):
                    contest.redis_client = redis_client
                    contest.user_manager = user_manager
                contests_closed += 1
                contest.end_contest()
                self.end_contest(contest, ledger_manager=ledger_manager)
        return contests_closed

    def hit_milestone_triggers(
        self,
        event: ContestEntryTrigger,
        user: User,
        country_iso: str,
        ledger_manager: ThlLedgerManager,
    ) -> PositiveInt:
        """For any open milestone contest that has a trigger on this event,
        if the user is eligible, hit it!
        """
        cs = self.get_many_by_user_eligible_milestone(
            user=user, country_iso=country_iso, entry_trigger=event
        )
        for c in cs:
            self.enter_milestone_contest(
                contest_uuid=c.uuid,
                country_iso=country_iso,
                user=user,
                ledger_manager=ledger_manager,
            )
        return len(cs)
