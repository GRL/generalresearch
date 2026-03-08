import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Collection, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID

import redis
from more_itertools import chunked, flatten
from pydantic import AwareDatetime, NonNegativeInt, PositiveInt
from redis.exceptions import LockError, LockNotOwnedError

from generalresearch.currency import LedgerCurrency
from generalresearch.managers import parse_order_by
from generalresearch.managers.base import (
    Permission,
    PostgresManager,
    RedisManager,
)
from generalresearch.managers.thl.ledger_manager.exceptions import (
    LedgerAccountDoesntExistError,
    LedgerTransactionConditionFailedError,
    LedgerTransactionCreateError,
    LedgerTransactionCreateLockError,
    LedgerTransactionDoesntExistError,
    LedgerTransactionFlagAlreadyExistsError,
    LedgerTransactionReleaseLockError,
)
from generalresearch.models.custom_types import UUIDStr, check_valid_uuid
from generalresearch.models.thl.ledger import (
    LedgerAccount,
    LedgerEntry,
    LedgerTransaction,
    UserLedgerTransactionType,
    UserLedgerTransactionTypesSummary,
)
from generalresearch.pg_helper import PostgresConfig
from generalresearch.redis_helper import RedisConfig

logging.basicConfig()
logger = logging.getLogger("LedgerManager")
logger.setLevel(logging.INFO)

# We can re-use this in any query that is retrieving full TXs
FULL_TX_JOINS = """
LEFT JOIN LATERAL (
    SELECT
        string_agg(tm.key || '=' || tm.value, '&') AS key_value_pairs
    FROM ledger_transactionmetadata tm
    WHERE tm.transaction_id = lt.id
) meta ON TRUE
LEFT JOIN LATERAL (
    SELECT
        jsonb_agg(
            jsonb_build_object(
                'direction', le.direction,
                'amount', le.amount,
                'account_id', le.account_id,
                'entry_id', le.id
            )
        ) AS entries_json
    FROM ledger_entry le
    WHERE le.transaction_id = lt.id
) entries ON TRUE
"""


class LedgerManagerBasePostgres(PostgresManager, RedisManager):
    def __init__(
        self,
        pg_config: PostgresConfig,
        redis_config: RedisConfig,
        permissions: Collection[Permission] = None,
        cache_prefix: Optional[str] = None,
        currency: Optional[LedgerCurrency] = LedgerCurrency.USD,
        testing: bool = False,
    ):
        if permissions is not None and (
            Permission.CREATE in permissions and redis_config is None
        ):
            raise ValueError("must pass redis_url when requesting CREATE permission")
        cache_prefix = cache_prefix or "ledger-manager"
        super().__init__(
            pg_config=pg_config,
            permissions=permissions,
            redis_config=redis_config,
            cache_prefix=cache_prefix,
        )
        self.currency = currency
        self.testing = testing
        if self.testing:
            self.currency = LedgerCurrency.TEST

    def make_filter_str(
        self,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
        account_uuid: Optional[str] = None,
        metadata_key: Optional[str] = None,
        metadata_value: Optional[str] = None,
    ):
        filters = []
        params = {}
        if time_start or time_end:
            time_end = time_end or datetime.now(tz=timezone.utc)
            time_start = time_start or datetime(2017, 1, 1, tzinfo=timezone.utc)
            assert time_start.tzinfo.utcoffset(time_start) == timedelta()
            assert time_end.tzinfo.utcoffset(time_end) == timedelta()
            filters.append("lt.created BETWEEN %(time_start)s AND %(time_end)s")
            params["time_start"] = time_start.replace(tzinfo=None)
            params["time_end"] = time_end.replace(tzinfo=None)
        if account_uuid:
            filters.append("le.account_id = %(account_uuid)s")
            params["account_uuid"] = account_uuid
        if metadata_key is not None:
            filters.append("key = %(metadata_key)s")
            params["metadata_key"] = metadata_key
        if metadata_value is not None:
            assert (
                metadata_key is not None
            ), "cannot filter by metadata_value without metadata_key"
            filters.append("value = %(metadata_value)s")
            params["metadata_value"] = metadata_value

        filter_str = "WHERE " + " AND ".join(filters) if filters else ""
        return filter_str, params


class LedgerTransactionManager(LedgerManagerBasePostgres):

    def create_tx(
        self,
        entries: List[LedgerEntry],
        metadata: Optional[Dict[str, str]] = None,
        ext_description: Optional[str] = None,
        tag: Optional[str] = None,
        created: Optional[AwareDatetime] = None,
    ) -> LedgerTransaction:
        """
        :returns a LedgerTransaction ID. This is because we can't fully populate
            the response object with valid children (eg: Entries can't get
            their ID because of c.executemany only returns back a single
            lastrowid)
        """

        assert (
            Permission.CREATE in self.permissions
        ), "LedgerTransactionManager has insufficient Permissions"

        if metadata is None:
            metadata = dict()
        if created is None:
            created = datetime.now(tz=timezone.utc)

        t = LedgerTransaction(
            created=created,
            ext_description=ext_description,
            tag=tag,
            metadata=metadata,
            entries=entries,
        )
        d = t.model_dump_mysql(include={"created", "ext_description", "tag"})
        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                # (1) Insert the Ledger Tx into the DB
                c.execute(
                    """
                    INSERT INTO ledger_transaction
                    (created, ext_description, tag)
                    VALUES (%(created)s, %(ext_description)s, %(tag)s)
                    RETURNING id;
                """,
                    d,
                )
                t.id = c.fetchone()["id"]

                # (2) Associate any metadata with the recently created Ledger Tx in the DB
                metadata_values = [
                    {"key": k, "value": v, "transaction_id": t.id}
                    for k, v in metadata.items()
                ]
                c.executemany(
                    """
                    INSERT INTO ledger_transactionmetadata
                    (key, value, transaction_id)
                    VALUES (%(key)s, %(value)s, %(transaction_id)s)
                """,
                    metadata_values,
                )

                # (3) Create the Ledger Tx Entries in the DB
                for entry in entries:
                    entry.transaction_id = t.id
                entry_values = [entry.model_dump(mode="json") for entry in entries]
                c.executemany(
                    """
                    INSERT INTO ledger_entry
                        (direction, amount, account_id, transaction_id)
                    VALUES (%(direction)s, %(amount)s, %(account_uuid)s, 
                        %(transaction_id)s)
                """,
                    entry_values,
                )
            conn.commit()
        return t

    def create_tx_protected(
        self,
        lock_key: str,
        condition: Callable[..., Union[bool, Tuple[bool, str]]],
        create_tx_func: Callable,
        flag_key: Optional[str] = None,
        skip_flag_check: bool = False,
    ) -> LedgerTransaction:
        """
        The complexity here is that even we protect a transaction logic with
            a lock, if two workers try to create the same transaction at a time,
            the lock just prevents them from doing it simultaneously; they will
            instead just do it sequentially. To prevent this, we can pass in a
            conditional that evaluates AFTER the lock is acquired, and we break
            if the condition is not met.

        1) Try to acquire a lock. If the lock is currently held, quit. All the
            following within held lock.
        2) Check if the flag exists. If so, quit.
        3) Check if the condition is True. If not, quit. (we have to do this
            b/c the flag may have expired out of redis).
        4) Create transaction
        5) Release lock

        :param lock_key: A str that should protect the conditions we are
            checking. Could be unique for this specific transaction or account.
            e.g. For a user cashing out their wallet balance, we would lock on
            the user.uuid, for paying for a task, we'd use the wall/session
            uuid.
        :param flag_key: A str that should be unique for this specific
            transaction only (used for de-dupe purposes). e.g. User is cashing
            out their wallet. We lock using the user's uuid, so they can't
            cashout 2 different txs at the same time (and result in a negative
            wallet balance). The flag is set based on the tx's ID just as a
            quick de-dupe check to make sure we don't run the same tx twice.
        :param condition: A function that gets run once we acquire the lock. It
            should return True if we want to continue with creating a new tx.
            The LedgerManager will get passed in as the first and only argument.
            The condition should do things like 1) check if the tx is already
            in the db, and/or 2) check if the account has sufficient funds to
            cover the tx, for e.g.
        :param create_tx_func: The function to run that creates the transaction.
        :param skip_flag_check: If create_tx_func or condition call fails, the
            flag would get set even though the transaction did not get created.
            If we want to manually re-try it, we need to skip the flag check.

        :return: The transaction that was created (or raise
            a LedgerTransactionCreateError())
        """
        rc = self.redis_client
        lock_name = f"{self.cache_prefix}:transaction_lock:{lock_key}"
        if flag_key is None:
            flag_name = f"{self.cache_prefix}:transaction_flag:{lock_key}"
        else:
            flag_name = f"{self.cache_prefix}:transaction_flag:{flag_key}"

        # The lock is NOT blocking (by default). So if we can't acquire the
        #   lock immediately, it means someone else has it already, and is
        #   probably executing this transaction, so quit.
        # The timeout is how long before the redis lock key expires, which
        #   would only happen if we didn't exit the `with` block normally
        #   (exiting the `with` block normally clears the lock key).
        # We could also have `blocking_timeout`, which is how long to wait
        #   until we can acquire a lock, but this is used only if `blocking`
        #   is True.
        #
        # There is nothing here limiting how long we can spend working within
        #   the `with` block.
        try:
            tx_created = False
            with rc.lock(
                name=lock_name,
                timeout=10,
                blocking=False,
                blocking_timeout=None,
            ):
                if skip_flag_check is False and rc.get(flag_name):
                    raise LedgerTransactionFlagAlreadyExistsError()

                # Maybe the flag set should be moved after the create_tx_func()?
                #   If we do that however, if the condition is failing and
                #   taking a long time, this would allow the tx to get retried
                #   over and over every 4 seconds, which is not good.
                rc.set(name=flag_name, value=1, ex=3600 * 24)

                # Condition returns either bool or Tuple[bool, str]
                condition_res = condition(self)

                if isinstance(condition_res, tuple):
                    condition_res, condition_msg = condition_res
                else:
                    condition_msg = ""

                if condition_res is False:
                    rc.delete(flag_name)
                    raise LedgerTransactionConditionFailedError(condition_msg)

                tx = create_tx_func()
                tx_created = True

        except LockNotOwnedError:
            # This happens if there is an error trying to release the lock.
            #   The tx most likely was created.
            raise LedgerTransactionReleaseLockError()

        except LockError as e:
            # There was an error acquiring the lock. The `with` block
            #   did not run.
            logger.log(level=logging.ERROR, msg=str(e))
            rc.delete(flag_name)
            raise LedgerTransactionCreateLockError()

        except redis.exceptions.RedisError as e:
            if not tx_created:
                # Redis failed before tx was created. Could be either on lock acquire, on
                #   flag check (get or set), on anything in the condition checks.
                rc.delete(flag_name)
                raise LedgerTransactionCreateError(f"Redis error: {e}")
            else:
                # Most likely redis fail on lock release. The tx was already created!
                raise LedgerTransactionReleaseLockError(f"Redis error: {e}")

        return tx

    def get_tx_ids_by_tag(self, tag: str) -> set[PositiveInt]:
        """`tag` is not a unique field, so it may return more than 1
        transaction. It should NOT return a substantial number of
        transactions. Use filtering by metadata for that purpose.

        returns: a list of id, not the full transactions
        """

        assert ":" in tag, "Please confirm the tag is valid"
        assert len(tag) > 6, "Please confirm the tag is valid"

        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT lt.id
                FROM ledger_transaction AS lt
                WHERE tag = %s
                LIMIT 101
            """,
            params=[tag],
        )
        if len(res) > 100:
            raise ValueError(f"Too many txs with this tag: {tag}")
        return {x["id"] for x in res}

    def get_tx_by_tag(self, tag: str) -> List[LedgerTransaction]:
        tx_ids = self.get_tx_ids_by_tag(tag=tag)
        return self.get_tx_by_ids(transaction_ids=tx_ids)

    def get_tx_ids_by_tags(self, tags: List[str]) -> set[PositiveInt]:
        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT lt.id, lt.tag, lt.created, lt.ext_description
                FROM ledger_transaction AS lt
                WHERE tag = ANY(%s)
            """,
            params=[list(tags)],
        )

        return {x["id"] for x in res}

    def get_txs_by_tags(self, tags: List[str]) -> List[LedgerTransaction]:
        tx_ids = self.get_tx_ids_by_tags(tags=tags)
        return self.get_tx_by_ids(transaction_ids=tx_ids)

    def get_tx_by_id(self, transaction_id: PositiveInt) -> LedgerTransaction:
        assert isinstance(transaction_id, int), "transaction_id must be an PositiveInt"

        res = self.get_tx_by_ids(transaction_ids=[transaction_id])

        if len(res) != 1:
            raise LedgerTransactionDoesntExistError

        return res[0]

    def get_tx_by_ids(
        self,
        transaction_ids: Collection[PositiveInt],
    ) -> List[LedgerTransaction]:

        args = {"transaction_ids": list(transaction_ids)}

        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT
                lt.id AS transaction_id,
                lt.created,
                lt.ext_description,
                lt.tag,
                meta.key_value_pairs,
                entries.entries_json
          FROM ledger_transaction lt
            {FULL_TX_JOINS}
          WHERE lt.id = ANY(%(transaction_ids)s);
          """,
            params=args,
        )
        return self.process_get_tx_mysql_rows_json(res)

    @staticmethod
    def process_get_tx_mysql_rows_json(
        rows: Collection[Dict[str, Any]],
    ) -> List[LedgerTransaction]:
        """Columns: transaction_id, created, ext_description, tag,
            key_value_pairs, entries_json
        - key_value_pairs: &-delimited key=value pairs
        - entries_json: list of objects, containing keys: direction,
        amount, entry_id, account_id

        """
        txs = []
        for row in rows:
            if row["key_value_pairs"]:
                metadata = {
                    key: value
                    for key, value in (
                        pair.split("=") for pair in row["key_value_pairs"].split("&")
                    )
                }
            else:
                metadata = dict()

            entries = [
                LedgerEntry(
                    id=e["entry_id"],
                    amount=e["amount"],
                    direction=e["direction"],
                    account_uuid=UUID(e["account_id"]).hex,
                    transaction_id=row["transaction_id"],
                )
                # Don't assume a Tx has Entries. We have cleanup methods that
                #   try to delete Tx if they failed (eg: during bp_payout)
                #   and we can't guarantee 2 entries per Tx
                for e in row.get("entries_json", [])
            ]
            txs.append(
                LedgerTransaction(
                    id=row["transaction_id"],
                    entries=entries,
                    metadata=metadata,
                    created=row["created"].replace(tzinfo=timezone.utc),
                    ext_description=row["ext_description"],
                    tag=row["tag"],
                )
            )
        return txs

    def get_tx_filtered_by_account_summary(
        self,
        account_uuid: UUIDStr,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> UserLedgerTransactionTypesSummary:
        filter_str, params = self.make_filter_str(
            time_start=time_start,
            time_end=time_end,
        )
        params["account_uuid"] = account_uuid

        # We do direction * -1 b/c the values here are w.r.t the user.
        # noinspection SqlShouldBeInGroupBy
        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT
                tmd.value AS tx_type,
                COUNT(1)  AS entry_count,
                MIN(le.amount * le.direction * -1) AS min_amount,
                MAX(le.amount * le.direction * -1) AS max_amount,
                SUM(le.amount * le.direction * -1) AS total_amount
            FROM ledger_transaction lt
            JOIN ledger_entry le
                  ON le.transaction_id = lt.id
                 AND le.account_id = %(account_uuid)s
            JOIN ledger_transactionmetadata tmd
                  ON tmd.transaction_id = lt.id
                 AND tmd.key = 'tx_type'
            {filter_str}
            GROUP BY tmd.value
            ORDER BY tmd.value;
            """,
            params=params,
        )
        d = {x["tx_type"]: x for x in res}
        return UserLedgerTransactionTypesSummary.model_validate(d)

    def get_tx_filtered_by_account_count(
        self,
        account_uuid: UUIDStr,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> NonNegativeInt:
        filter_str, params = self.make_filter_str(
            time_start=time_start,
            time_end=time_end,
        )
        params["account_uuid"] = account_uuid

        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT COUNT(DISTINCT lt.id) as cnt
            FROM ledger_transaction lt
            JOIN ledger_entry le
              ON le.transaction_id = lt.id
             AND le.account_id = %(account_uuid)s
            {filter_str}
            """,
            params=params,
        )
        return res[0]["cnt"] if res else 0

    def get_tx_filtered_by_account(
        self,
        account_uuid: UUIDStr,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
        order_by: Optional[str] = "created,tag",
    ) -> List[LedgerTransaction]:
        txs, _ = self.get_tx_filtered_by_account_paginated(
            account_uuid=account_uuid,
            time_start=time_start,
            time_end=time_end,
            order_by=order_by,
        )
        return txs

    def get_balance_before_page(
        self,
        account_uuid: str,
        oldest_created: datetime,
        exclude_txs_before: Optional[datetime] = None,
    ) -> NonNegativeInt:
        """
        In a paginated list of txs, if I want to calculate
        a running balance, I need the balance in that account
        starting at the oldest tx in the page
        This is identical to get_account_balance_timerange
        """
        params = {
            "account_uuid": account_uuid,
            "oldest_created": oldest_created,
        }
        exclude_str = ""
        if exclude_txs_before:
            exclude_str = "AND lt.created > %(exclude_txs_before)s"
            params["exclude_txs_before"] = exclude_txs_before
        query = f"""
        SELECT
            COALESCE(SUM(le.amount * le.direction * la.normal_balance), 0) AS balance_before_page
        FROM ledger_transaction lt
        JOIN ledger_entry le
          ON le.transaction_id = lt.id
        JOIN ledger_account la
          ON la.uuid = le.account_id
        WHERE le.account_id = %(account_uuid)s
          AND lt.created < %(oldest_created)s
          {exclude_str};"""
        res = self.pg_config.execute_sql_query(query, params=params)

        return res[0]["balance_before_page"]

    def include_running_balance(
        self,
        txs: List[UserLedgerTransactionType],
        account_uuid: str,
        exclude_txs_before: Optional[AwareDatetime] = None,
    ):
        """
        exclude_txs_before is NOT for filtering. It is a "hack" to exclude
        transactions from before a certain date for balance consideration.
        """
        if len(txs) == 0:
            return txs
        oldest_created = min([x.created for x in txs])
        balance_before_page = self.get_balance_before_page(
            account_uuid=account_uuid,
            oldest_created=oldest_created,
            exclude_txs_before=exclude_txs_before,
        )
        page_with_idx = list(enumerate(txs))
        page_with_idx.sort(key=lambda x: x[1].created)
        balance = balance_before_page
        for _, tx in page_with_idx:
            balance += tx.amount
            tx.balance_after = balance
        # restore original order
        page_with_idx.sort(key=lambda x: x[0])
        txs = [tx for _, tx in page_with_idx]
        return txs

    def get_tx_filtered_by_account_paginated(
        self,
        account_uuid: UUIDStr,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order_by: Optional[str] = "created,tag",
    ) -> Tuple[List[LedgerTransaction], int]:
        """
        If time_start and/or time_end are passed, the txs are filtered to
            include only that range.
          - time_start is optional, default = beginning of time
          - time_end is optional, default = now

        If page/size are passed, return only that page of the filtered (by
            account_uuid and optionally time) items.

        Returns (list of items, total (after filtering)).
         :param account_uuid: Will return txs that have a ledger entry that touches this account
         :param time_start: Filter to include this range. Default: beginning of time
         :param time_end: Filter to include this range. Default: now
         :param page: page starts at 1
         :param size: size of page, default (if page is not None) = 100. (1<=page<=100)
         :param order_by: Required for pagination. Uses django-rest-framework ordering syntax,
            e.g. '-created,tag' for (created desc, tag asc)
        """

        assert isinstance(account_uuid, str), "account_uuid must be a str"
        check_valid_uuid(account_uuid)

        filter_str, params = self.make_filter_str(
            time_start=time_start, time_end=time_end, account_uuid=account_uuid
        )

        if page is not None:
            assert type(page) is int
            assert page >= 1, "page starts at 1"
            size = size if size is not None else 100
            assert type(size) is int
            assert 1 <= size <= 100
            params["offset"] = (page - 1) * size
            params["limit"] = size
            paginated_filter_str = " LIMIT %(limit)s OFFSET %(offset)s"
            total = self.get_tx_filtered_by_account_count(
                account_uuid=account_uuid,
                time_start=time_start,
                time_end=time_end,
            )
        else:
            paginated_filter_str = ""
            # Don't need to do a count if we aren't paginating
            total = None

        order_by_str = parse_order_by(order_by)

        res = self.pg_config.execute_sql_query(
            query=f"""
            WITH tx_ids AS (
                SELECT lt.id
                FROM ledger_transaction lt
                JOIN ledger_entry le
                  ON le.transaction_id = lt.id
                {filter_str}
                GROUP BY lt.id, lt.created
                {order_by_str}
                {paginated_filter_str}
            )
            SELECT
                lt.id AS transaction_id,
                lt.created,
                lt.ext_description,
                lt.tag,
                meta.key_value_pairs,
                entries.entries_json
            
            FROM tx_ids t
            JOIN ledger_transaction lt ON lt.id = t.id
            
            {FULL_TX_JOINS}
            
            {order_by_str};
          """,
            params=params,
        )
        if total is None:
            total = len(res)

        return (
            self.process_get_tx_mysql_rows_json(res),
            total,
        )

    def get_tx_filtered_by_metadata(
        self,
        metadata_key: str,
        metadata_value: str,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> List[LedgerTransaction]:
        # Renamed from "get_tx_filtered" which is not a good name

        filter_str, params = self.make_filter_str(
            time_start=time_start,
            time_end=time_end,
            metadata_key=metadata_key,
            metadata_value=metadata_value,
        )

        res = self.pg_config.execute_sql_query(
            query=f"""
            WITH tx_ids AS (
                SELECT DISTINCT lt.id
                FROM ledger_transaction lt
                JOIN ledger_transactionmetadata ltm
                  ON ltm.transaction_id = lt.id
                {filter_str}
            )
            SELECT
                lt.id AS transaction_id,
                lt.created,
                lt.ext_description,
                lt.tag,
                meta.key_value_pairs,
                entries.entries_json
            
            FROM tx_ids t
            JOIN ledger_transaction lt ON lt.id = t.id
            {FULL_TX_JOINS}
          """,
            params=params,
        )

        return LedgerTransactionManager.process_get_tx_mysql_rows_json(res)


class LedgerMetadataManager(LedgerManagerBasePostgres):
    """
    WARNING: TxtMetadata doesn't have an official Pydantic model
        definition. So this is going to operate primarily on Dicts
    """

    def get_tx_metadata_by_txs(
        self, transactions: List[LedgerTransaction]
    ) -> Dict[PositiveInt, Dict[str, Any]]:
        """
        Each transaction can have 1 metadata dictionary. However, each
        metadata dictionary can have multiple key/value pairs that
        corresponds to each metadata row in the database.

        """

        tx_ids = set([tx.id for tx in transactions])
        res = self.pg_config.execute_sql_query(
            query="""
                SELECT 
                    tx_meta.id, tx_meta.key, 
                    tx_meta.value, tx_meta.transaction_id
                FROM ledger_transactionmetadata AS tx_meta
                WHERE tx_meta.transaction_id = ANY(%s)
            """,
            params=[list(tx_ids)],
        )

        metadata = defaultdict(dict)
        for x in res:
            metadata[x["transaction_id"]][x["key"]] = x["value"]

        return metadata

    def get_tx_metadata_ids_by_tx(
        self, transaction: LedgerTransaction
    ) -> Set[PositiveInt]:
        return self.get_tx_metadata_ids_by_txs(transactions=[transaction])

    def get_tx_metadata_ids_by_txs(
        self, transactions: List[LedgerTransaction]
    ) -> Set[PositiveInt]:
        """
        This explicitly returns the tx_metadata database ids. Potentially,
        useful for counting total key/value pairs, and/or deleting records
        from the database.
        """

        tx_ids = set([tx.id for tx in transactions])
        res = self.pg_config.execute_sql_query(
            query="""
                SELECT tx_meta.id 
                FROM ledger_transactionmetadata AS tx_meta
                WHERE tx_meta.transaction_id = ANY(%s)
            """,
            params=[list(tx_ids)],
        )

        return set([i["id"] for i in res])


class LedgerEntryManager(LedgerManagerBasePostgres):

    def get_tx_entries_by_tx(self, transaction: LedgerTransaction) -> List[LedgerEntry]:
        return self.get_tx_entries_by_txs(transactions=[transaction])

    def get_tx_entries_by_txs(
        self, transactions: List[LedgerTransaction]
    ) -> List[LedgerEntry]:
        tx_ids = set([tx.id for tx in transactions])
        tx_entries = self.pg_config.execute_sql_query(
            query="""
                SELECT 
                    entry.id, entry.direction, entry.amount, 
                    entry.account_id as account_uuid, 
                    entry.transaction_id
                FROM ledger_entry AS entry
                WHERE entry.transaction_id = ANY(%s)
            """,
            params=[list(tx_ids)],
        )

        return [LedgerEntry.model_validate(res) for res in tx_entries]


class LedgerAccountManager(LedgerManagerBasePostgres):
    """This Manager class is primarily involved with any operations on the
    ledger_account table within the ledger system.

    We have Ledger Accounts for many different purposes,

    """

    def create_account(self, account: LedgerAccount) -> LedgerAccount:
        assert (
            Permission.CREATE in self.permissions
        ), "LedgerManager does not have sufficient permissions"

        d = account.model_dump(mode="json")

        # These we're excluded, so manually reassign them to the mysql args
        d["reference_type"] = account.reference_type
        d["qualified_name"] = account.qualified_name

        self.pg_config.execute_write(
            query="""
            INSERT INTO ledger_account
                (uuid, display_name, qualified_name, account_type,
                normal_balance, reference_type, reference_uuid, 
                currency)
            VALUES (%(uuid)s, %(display_name)s, %(qualified_name)s, 
                %(account_type)s, %(normal_balance)s, %(reference_type)s, 
                %(reference_uuid)s, %(currency)s)
                """,
            params=d,
        )

        return account

    def get_account(
        self, qualified_name: str, raise_on_error: bool = True
    ) -> Optional[LedgerAccount]:
        res = self.get_account_many(
            qualified_names=[qualified_name], raise_on_error=raise_on_error
        )
        return res[0] if len(res) == 1 else None

    def get_account_many_(
        self, qualified_names: List[str], raise_on_error: bool = True
    ) -> List[Dict[str, Any]]:
        assert len(qualified_names) <= 500, "chunk me"

        # qualified_name has a unique index so there can only be 0 or 1 match.
        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT  
                uuid, display_name, qualified_name, account_type, 
                normal_balance, reference_type,  
                reference_uuid, currency
            FROM ledger_account
            WHERE qualified_name = ANY(%s); 
            """,
            params=[qualified_names],
        )

        if raise_on_error and (not res or len(res) != len(qualified_names)):
            raise LedgerAccountDoesntExistError

        return list(res)

    def get_account_many(
        self, qualified_names: List[str], raise_on_error: bool = True
    ) -> List[LedgerAccount]:
        res = flatten(
            [
                self.get_account_many_(chunk, raise_on_error)
                for chunk in chunked(qualified_names, 500)
            ]
        )
        return [LedgerAccount.model_validate(i) for i in res]

    def get_account_or_create(self, account: LedgerAccount) -> LedgerAccount:
        res: Optional[LedgerAccount] = self.get_account(
            qualified_name=account.qualified_name, raise_on_error=False
        )
        return res or self.create_account(account=account)

    def get_accounts(self, qualified_names: List[str]) -> List[LedgerAccount]:
        return self.get_account_many(qualified_names, raise_on_error=True)

    def get_accounts_if_exists(self, qualified_names: List[str]) -> List[LedgerAccount]:
        """Rather than returning None, this may return an empty list, or
        a list that has less LedgerAccount instances than the number of
        qualified_names that was passed in.
        """
        return self.get_account_many(qualified_names, raise_on_error=False)

    def get_account_if_exists(self, qualified_name: str) -> Optional[LedgerAccount]:
        return self.get_account(qualified_name, raise_on_error=False)

    def get_account_balance(self, account: LedgerAccount) -> int:
        """In a debit normal account, the balance is the sum of debits minus
        the sum of credits.

        In a credit normal account, the balance is the sum of credits minus
        the sum of debits.

        This returns an int and not a USDCent because an Account's balance
        could be negative.
        """

        # TODO: Move to RR with long timeout (2min+), it causes problems
        res = self.pg_config.execute_sql_query(
            query=f"""
                SELECT SUM(amount * direction) AS total
                FROM ledger_entry
                WHERE account_id = %s
            """,
            params=[account.uuid],
        )
        if res:
            return int((res[0]["total"] or 0) * account.normal_balance)
        else:
            return 0

    def get_account_balance_timerange(
        self,
        account: LedgerAccount,
        time_start: Optional[AwareDatetime] = None,
        time_end: Optional[AwareDatetime] = None,
    ) -> int:
        """
        This returns an int and not a USDCent because an Account's balance
        could be negative.
        """

        # I want the balance for this account optionally filtered by
        # transactions within a time range
        filter_str, params = self.make_filter_str(
            account_uuid=account.uuid, time_start=time_start, time_end=time_end
        )

        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT SUM(amount * direction * normal_balance) AS total
            FROM ledger_entry AS le
            JOIN ledger_transaction AS lt 
                ON le.transaction_id = lt.id
            JOIN ledger_account AS la 
                ON le.account_id = la.uuid
            {filter_str}
        """,
            params=params,
        )
        if not res:
            return 0
        return int(res[0]["total"]) if res[0]["total"] else 0

    def get_account_filtered_balance(
        self,
        account: LedgerAccount,
        metadata_key: str,
        metadata_value: str,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> int:
        """I want the balance for this account filtered by transactions with
        a certain tag.

        NOTE: This query will be wrong if the metadata join was changed!
        b/c if a transaction had multiple matching metadata rows, then the
        ledger_entry row will get returned multiple times and the
        account_balance will be SUMmed wrong!

        This returns an int and not a USDCent because an Account's balance
        could be negative.
        """
        filter_str, params = self.make_filter_str(
            account_uuid=account.uuid,
            time_start=time_start,
            time_end=time_end,
            metadata_key=metadata_key,
            metadata_value=metadata_value,
        )

        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT SUM(amount * direction * normal_balance) AS total
            FROM ledger_entry AS le
            JOIN ledger_transaction AS lt 
                ON le.transaction_id = lt.id
            JOIN ledger_transactionmetadata AS tm 
                ON lt.id = tm.transaction_id
            JOIN ledger_account AS la 
                ON le.account_id = la.uuid
            {filter_str}
        """,
            params=params,
        )
        if not res:
            return 0

        return int(res[0]["total"]) if res[0]["total"] else 0


class LedgerManager(
    LedgerTransactionManager,
    LedgerEntryManager,
    LedgerAccountManager,
    LedgerMetadataManager,
):
    """This is the parent class manager for operating within the ledger
    app. Many of the methods that are in here are unused, and written
    when it was unclear what queries would be needed.

    As of discussion on 2025-05-01, more functionality should be put into
    the TransactionManger, AccountManger, and even the creation of a
    EntryManager or TransactionMetadataManger with various "verbose"
    flags to determine how different relationships are returned. For
    example, the TransactionManger doesn't need to always include details
    about each Entry.

    Given that there is a "THL Ledger" the goal of these classes should be very
    simple and related to the ledger itself, not any specific application

    """

    def check_ledger_balanced(self) -> bool:
        """This is for testing only, as it'll take forever to run this if
        the ledger_manager is huge
        """
        res = self.pg_config.execute_sql_query(
            f"""
            SELECT
                SUM(CASE WHEN normal_balance = -1 THEN total ELSE 0 END) AS credit_total,
                SUM(CASE WHEN normal_balance = 1 THEN total ELSE 0 END)  AS debit_total
            FROM (
                SELECT 
                    SUM(amount * direction * normal_balance) AS total, 
                    tl.normal_balance
                FROM ledger_entry
                JOIN ledger_account tl 
                    ON ledger_entry.account_id = tl.uuid
                GROUP BY account_id, normal_balance
            ) x
        """
        )[0]
        return res["credit_total"] == res["debit_total"]

    def get_account_debit_credit_by_metadata(
        self,
        account: LedgerAccount,
        metadata_key: str,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
    ) -> Dict[str, Dict[str, int]]:
        """Show me the sum of debit and credit scoped to this account, grouped
        by all values of metadata_key
        """
        filter_str, params = self.make_filter_str(
            account_uuid=account.uuid,
            metadata_key=metadata_key,
            time_end=time_end,
            time_start=time_start,
        )

        # noinspection SqlShouldBeInGroupBy
        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT 
                SUM(CASE WHEN direction = 1 THEN amount ELSE 0 END)  AS debit,
                SUM(CASE WHEN direction = -1 THEN amount ELSE 0 END) AS credit,
                tm.value
            FROM ledger_entry AS le
            JOIN ledger_transaction AS lt 
                ON le.transaction_id = lt.id
            JOIN ledger_transactionmetadata AS tm 
                ON lt.id = tm.transaction_id
            JOIN ledger_account AS la 
                ON le.account_id = la.uuid
            {filter_str}
            GROUP BY tm.value
        """,
            params=params,
        )
        if not res:
            return {}
        return {
            x["value"]: {"credit": int(x["credit"]), "debit": int(x["debit"])}
            for x in res
        }

    def get_balances_timerange(
        self,
        time_start: Optional[AwareDatetime] = None,
        time_end: Optional[AwareDatetime] = None,
    ) -> Dict[str, Any]:

        filter_str, params = self.make_filter_str(
            time_end=time_end,
            time_start=time_start,
        )

        # noinspection SqlShouldBeInGroupBy
        res = self.pg_config.execute_sql_query(
            query=f"""
            SELECT
                la.*, 
                SUM(CASE WHEN direction = 1 THEN amount ELSE 0 END)  AS debit,
                SUM(CASE WHEN direction = -1 THEN amount ELSE 0 END) AS credit
            FROM ledger_entry AS le
            JOIN ledger_transaction AS lt
                ON le.transaction_id = lt.id
            JOIN ledger_account AS la 
                ON le.account_id = la.uuid
            {filter_str}
            GROUP BY la.uuid
        """,
            params=params,
        )
        d = {
            LedgerAccount.model_validate(x): {
                "debit": x["debit"],
                "credit": x["credit"],
            }
            for x in res
        }
        for k, v in d.items():
            v["total"] = (v["debit"] - v["credit"]) * k.normal_balance.value
        return d
