import uuid

from django.db import models
from django.db.models import Q


class THLSession(models.Model):
    """
    The top level table is a session. Instead of only containing an ID, it'll have an internal
    auto-increment, and an external UUID (that is for the session itself, not shared with the Wall ids).
    """

    id = models.BigAutoField(primary_key=True, null=False)

    # This is what gets exposed externally
    uuid = models.UUIDField(null=False, unique=True)

    # The user that started this session
    user_id = models.BigIntegerField(null=False)

    # Makes it easy to look at session total elapsed time
    started = models.DateTimeField(null=False)
    finished = models.DateTimeField(null=True)

    # These are the promised "parameters" of the session, as specified by the
    # clicked bucket. User clicked on a bucket with LOI between these values.
    loi_min = models.SmallIntegerField(null=True)
    loi_max = models.SmallIntegerField(null=True)

    # User clicked on a bucket with a promised payout between these values.
    # This is the user_payout, which is the bp_payout with the
    # user_payout_transformation applied.
    user_payout_min = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    user_payout_max = models.DecimalField(max_digits=5, decimal_places=2, null=True)

    # This is a shortcut for us to easily see country specific activity, it's
    # set when the user enters the thl_session. We assert elsewhere in the code
    # that prevents a user from changing country_iso while in a survey
    country_iso = models.CharField(max_length=2, null=True)

    # The user's device type as determined by their useragent (nullable for
    #  legacy reasons)
    device_type = models.SmallIntegerField(null=True)

    # The user's latest IP address when starting this session (nullable for
    #  legacy reasons)
    ip = models.GenericIPAddressField(null=True)

    # The GRL status of the session. This is reportable externally. This can be
    # independent of the status of the final wall event. Possible values:
    #   NULL (enter) f (failure) c (complete) a (user exited, or started
    #   another session) t (timeout).
    # The status can only ever change from NULL-> something, and once it is
    # set, cannot change.
    status = models.CharField(max_length=1, default=None, null=True)

    # This is a more detailed status for the session. Again, may be unrelated
    # to any associated wall events, or there may even be NO wall events
    # (GRL Fail). This is also reportable externally.
    #
    # This is calculated from the underlying wall events' status codes. We may
    #   just report the last wall event's status_code, or maybe the most
    #   common? Or: If the user entered any client survey, it is a buyer_fail.
    #   Otherwise, it is usually the last wall event's.
    #
    # Uses GRL's category: (only those where status = 'f' would have a status_code)
    #   Buyer Fail: User terminated in buyer survey
    #   Buyer Quality Fail: User terminated in buyer survey for quality reasons
    #   PS Term: User failed in marketplace's pre-screener
    #   PS Quality Term: User rejected by marketplace for quality reasons
    #   PS OverQuota: User rejected by marketplace
    #   PS Duplicate: User rejected by marketplace
    #   GRL Fail: User was never sent into a marketplace (generally quality
    #       reasons, or user answered questions that made them ineligible for
    #       the requested survey)
    #
    # Note: PS OverQuota is theoretically "our" fault, and we don't want to
    #   expose that? Maybe we map it to PS Term when we expose it?
    status_code_1 = models.SmallIntegerField(null=True)

    # If the status_code_1 is GRL Fail, we could include another reason for the
    #   failure, such as VPN Usage, user is blocked, no eligible surveys, etc.
    status_code_2 = models.SmallIntegerField(null=True)

    # This may or may not be related to the CPI of the final survey. A session
    #   could have a payout even if the final survey was not a complete. This
    #   is the amount paid to the BP. It does not change, even if the session
    #   is reversed.
    #   -- Do we set it to 0.00 once the session is over?
    payout = models.DecimalField(max_digits=5, decimal_places=2, null=True)

    # The amount the BP should pay to the user. Only is set if configured by the BP.
    user_payout = models.DecimalField(max_digits=5, decimal_places=2, null=True)

    # This is the most recent reconciliation status of the session. Generally,
    #   we would adjust this if the last survey in the session was adjusted
    #   from complete to incomplete.
    #
    # Possible values: 'ac' (adjusted to complete), 'af' (adj to fail)
    adjusted_status = models.CharField(max_length=2, null=True)

    # If the session is 'fail' -> 'adj. to complete': payout is NULL (or 0?),
    #   adjusted_payout is the amount paid if 'complete' -> 'adj. to fail':
    #   payout is the amount, adjusted_payout is 0.00
    #
    # If a survey is complete, and then adjusted to incomplete, and then back
    #   to complete, then both adjusted_status and adjusted_payout would go
    #   back to NULL, however the adjusted_timestamp would be set!
    adjusted_payout = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    adjusted_user_payout = models.DecimalField(
        max_digits=5, decimal_places=2, null=True
    )

    # This timestamp gets updated every time there is an adjustment (it is
    # the latest)
    adjusted_timestamp = models.DateTimeField(null=True)

    # Wall Session Metadata. This is a passthrough of any extra arguments the
    #   BP appends on the offerwall request.
    url_metadata = models.JSONField(null=True)

    class Meta:
        db_table = "thl_session"
        indexes = [
            # For rolling window searches
            models.Index(fields=["user_id", "started"]),
            models.Index(fields=["started"]),
            # Used primarily for "dashboard"-related tasks, where we would
            #   filter by started first and the group by one of these field.
            models.Index(fields=["country_iso"]),
            models.Index(
                fields=["adjusted_status"],
                name="thl_session_adj_status_nn_idx",
                condition=Q(adjusted_status__isnull=False),
            ),
            models.Index(
                fields=["adjusted_timestamp"],
                name="thl_session_adj_ts_nn_idx",
                condition=Q(adjusted_timestamp__isnull=False),
            ),
            models.Index(fields=["device_type"]),
            models.Index(fields=["ip"]),
            # uuid will already have an index due to unique
        ]


class THLWall(models.Model):
    """
    A wall event must always exist within a session.
    """

    # This is what gets exposed externally (to marketplaces), it's also what
    # we'll map over from wall.mid. We need this for the marketplace redirects.
    uuid = models.UUIDField(primary_key=True)

    # We can use a ForeignKey here because we want these two tables
    # connected with a key constraint
    session = models.ForeignKey(
        THLSession,
        on_delete=models.RESTRICT,
        null=False,
        related_name="session",
    )

    # This is the marketplace we sent user to. len=2 for us to potentially expand.
    source = models.CharField(max_length=2, null=False)

    # Buyer / account within the marketplace / source
    buyer_id = models.CharField(max_length=32, null=True)

    # When we create the wall event, we set both survey_id & req_survey_id to
    #   the same value. If the user comes back from the redirect from a
    #   different survey_id, we'll change the survey_id to the
    #   "returned"/"actual" survey_id and req_survey_id unchanged.
    survey_id = models.CharField(max_length=32, null=False)
    req_survey_id = models.CharField(max_length=32, null=False)

    # This works the exact same as survey_id / req_survey_id. This CPI 2includes
    #   any applicable marketplace commission.
    # (It is possible they got sent elsewhere, or the CPI of the survey changed,
    # and it wasn't updated in time. If so, we'd update the cpi field).
    cpi = models.DecimalField(max_digits=8, decimal_places=5, null=False)
    req_cpi = models.DecimalField(max_digits=8, decimal_places=5, null=False)

    # thl_session.started does not necessarily equal thl_wall.started?
    started = models.DateTimeField(null=False)
    finished = models.DateTimeField(null=True)

    # The GRL status of the wall event. Possible values:
    #   NULL (enter) f (failure) c (complete) a (user exited, or started
    #   another session) t (timeout).
    #
    # The status can only ever change from NULL-> something, and once it is
    #   set, cannot change.
    status = models.CharField(max_length=1, default=None, null=True)

    # This is a more detailed status for the wall event. We will map each
    #   marketplace's status codes to one of these categories. Note: some
    #   marketplaces don't return enough information and so some marketplaces
    #   might only ever use a subset of these.
    #
    # Uses GRL's category: (only those where status = 'f' would have a status_code)
    #   Buyer Fail: User terminated in buyer survey
    #   Buyer Quality Fail: User terminated in buyer survey for quality reasons
    #   PS Term: User failed in marketplace's prescreener
    #   PS Quality Term: User rejected by marketplace for quality reasons
    #   PS OverQuota: User rejected by marketplace
    #   PS Duplicate: User rejected by marketplace
    status_code_1 = models.SmallIntegerField(null=True)

    # For future expansion
    status_code_2 = models.SmallIntegerField(null=True)

    # External status codes
    # This is the marketplace's status / status code / status reason / whatever
    # they call it.
    ext_status_code_1 = models.CharField(max_length=32, null=True)
    ext_status_code_2 = models.CharField(max_length=32, null=True)
    ext_status_code_3 = models.CharField(max_length=32, null=True)

    # The thl_wall event can be reported, without breaking the thl_session. A
    # user may want to report the first survey as invasive, but still continue
    report_value = models.SmallIntegerField(null=True)
    report_notes = models.CharField(max_length=255, null=True)

    # This is the most recent reconciliation status of the wall event.
    adjusted_status = models.CharField(max_length=2, null=True)

    # If the session is 'fail' -> 'adj. to complete': cpi is NULL (or 0?),
    #   adjusted_cpi is the amount paid if 'complete' -> 'adj. to fail': cpi
    #   is the amount, adjusted_cpi is 0.00
    adjusted_cpi = models.DecimalField(max_digits=8, decimal_places=5, null=True)
    adjusted_timestamp = models.DateTimeField(null=True)

    class Meta:
        db_table = "thl_wall"

        # We could start to do stuff like this to ensure a session doesn't
        # contain the same survey more than twice in the session
        unique_together = ("session", "source", "survey_id")

        # A session shouldn't have more than 100 wall events, or we should put
        #   additional indices. (session_id, started).

        indexes = [
            # uuid is primary key so already has an index
            models.Index(fields=["started"]),
            models.Index(fields=["source", "survey_id", "started"]),
            models.Index(
                fields=["adjusted_status"],
                name="thl_wall_adj_status_nn_idx",
                condition=Q(adjusted_status__isnull=False),
            ),
            models.Index(
                fields=["adjusted_timestamp"],
                name="thl_wall_adj_ts_nn_idx",
                condition=Q(adjusted_timestamp__isnull=False),
            ),
        ]


# # TODO: in the future
# class WallProgress(models.Model):
#     # Completion Percentage. We could have GRS send this, or calculate it
#       from the received answers, and/or direct buyer relationships could
#       send us this data.
#     progress = models.FloatField(null=True)
#     # Useful for tracking if a user is still "in" a survey.
#     progress_last_updated = models.DateTimeField(null=True)
#     # wall id
#     wall = models.ForeignKey


class THLUser(models.Model):
    """
    Class for the generic concept of a user in our entire platform
    """

    # This is the value that will get passed around internally to uniquely
    # identify a user. We'll never share this value outside of
    # General Research. Yes, it supports integers far larger
    # than the world's population. However, the additional storage overhead
    # is trivial and this table will likely get spammed with tons of signups
    # for users that will never be used.
    id = models.BigAutoField(primary_key=True, null=False, blank=False)

    # This uuid is what gets exposed anytime the user value is publicly
    # available. We don't use it for passing around internally because
    # it's large (32 char str). However, we keep it as the primary key so that
    # this table has less auto-increment issues
    uuid = models.UUIDField(null=False, blank=False, unique=True)

    # This is no longer a foreign key, so at least enforce they're UUIDs
    product_id = models.UUIDField(null=False, blank=False, unique=False)

    # We're going to limit the length of BPUID values to 128 characters.
    product_user_id = models.CharField(
        max_length=128, null=False, blank=False, unique=False
    )

    # ------------------
    # ---- METADATA ----
    # ------------------

    # This will be useful for looking at signups per country globally.
    created = models.DateTimeField(null=False)

    # This will be useful as our Daily Active User metric (DAU) that a lot
    # of marketplaces want reported on. We'll be able to change the
    # logic for when this is updated, but keeping it in a table will
    # make queries much easier.
    # We'll force null=True, and his will be the same as created
    # until they're "seen again".
    last_seen = models.DateTimeField(null=False)

    # this isn't used for any security measures, but we have an increasing
    # need to provide a "live panel book" to describe our user base. This
    # will help immediately filter users down by "after users from X country
    # in the past X days". Which is now nearly impossible.
    last_country_iso = models.CharField(max_length=2, null=True)

    # Along with the last_country_iso, we have the last_geoname_id which
    #   we could use to aggregate by state, timezone, continent, etc.
    # Note: Do not use PositiveIntegerField as this adds a constraint which
    #   prevents this column from being added instantly
    last_geoname_id = models.IntegerField(null=True)

    # Also for convenience, as this is available in the userhealth_iphistory
    # table, but we'd need to groupby/sort, so store the user's latest IP here
    last_ip = models.GenericIPAddressField(null=True)

    # No index needed on it, just a quick attribute check for if we process
    # additional resources for this user
    blocked = models.BooleanField(default=False)

    class Meta:
        db_table = "thl_user"

        # The same BPUID can't be present within a product_id
        unique_together = ("product_id", "product_user_id")

        indexes = [
            # id already has an index as the primary key
            # This will be used to look up a from GRS or
            #   possibly another "outside source". Does
            #   not need to be a composite with anything else.
            #   Note: Index is already created due to being marked unique
            # models.Index(fields=["uuid"]),
            # We will never look up a product_user_id by itself (because it's
            #   not unique), so this will always be a composite
            #   Note: Index is created by the `unique_together` above
            # models.Index(fields=["product_id", "product_user_id"]),
            models.Index(fields=["created"]),
            models.Index(fields=["last_seen"]),
            models.Index(fields=["last_country_iso"]),
        ]


class THLUserMetadata(models.Model):
    """
    Stores information about a user that is modifiable, including by the BP or
        potentially by the user itself. As opposed to the THLUser table
        which does not store fields that can be directly set.
    """

    # There is a one-to-one relationship between this and the THLUser table,
    #   so this id equals THLUser.id
    user = models.OneToOneField(
        to=THLUser, on_delete=models.RESTRICT, null=False, primary_key=True
    )

    email_address = models.CharField(max_length=320, null=True)
    email_sha256 = models.CharField(max_length=64, null=True)
    email_sha1 = models.CharField(max_length=40, null=True)
    email_md5 = models.CharField(max_length=32, null=True)

    class Meta:
        db_table = "thl_usermetadata"
        indexes = [
            models.Index(fields=["email_address"]),
            models.Index(fields=["email_sha256"]),
            models.Index(fields=["email_sha1"]),
            models.Index(fields=["email_md5"]),
        ]


class IPInformation(models.Model):
    """
    Most of the info in here can be imported from the City Plus csv
        (https://www.maxmind.com/en/geoip2-city), but we'll wait just use Insights.

    Using the Country DB files, we can only populate: ip, country_iso,
        registered_country_iso.

    If the IP address is in a tier 1 or 2 country, we'll call insights after.
        If the IP is in a tier 3 country, we'll call insights only if they
        actually enter a bucket.
    """

    ip = models.GenericIPAddressField(primary_key=True)

    # Use this to join on IPLocation table.
    #   In the insights API response, this is the city.geoname_id
    geoname_id = models.PositiveIntegerField(null=True)

    # This is duplicated in the IPLocation table, but keeping for convenience.
    #   This is the country the IP address is physically in, and is inferrable
    #   from the geoname_id.
    country_iso = models.CharField(max_length=2, blank=False, null=True)

    # The country in which the IP is registered (by the ISP)
    registered_country_iso = models.CharField(max_length=2, blank=False, null=True)

    # Traits (these come from Anonymous DB, through GeoIP2 Insights Web Service)
    # https://dev.maxmind.com/geoip/docs/databases/anonymous-ip
    is_anonymous = models.BooleanField(null=True)
    is_anonymous_vpn = models.BooleanField(null=True)
    is_hosting_provider = models.BooleanField(null=True)
    is_public_proxy = models.BooleanField(null=True)
    is_tor_exit_node = models.BooleanField(null=True)
    is_residential_proxy = models.BooleanField(null=True)

    # More Traits
    autonomous_system_number = models.IntegerField(null=True, blank=False)
    autonomous_system_organization = models.CharField(
        max_length=255, null=True, blank=False
    )
    domain = models.CharField(max_length=255, null=True, blank=True)
    isp = models.CharField(max_length=255, null=True, blank=False)
    mobile_country_code = models.CharField(max_length=3, null=True, blank=False)
    mobile_network_code = models.CharField(max_length=3, null=True, blank=False)
    network = models.CharField(max_length=56, null=True, blank=False)
    organization = models.CharField(max_length=255, null=True, blank=False)
    static_ip_score = models.FloatField(null=True)  # ranges from 0 to 99.99
    user_type = models.CharField(max_length=64, null=True, blank=False)
    # Leaving this out as it will be immediately out of date unless we keep this updated
    # user_count = models.PositiveIntegerField(null=True)

    # Location fields that may be different for different IPs in the same City
    postal_code = models.CharField(max_length=20, blank=True, null=True)
    latitude = models.DecimalField(max_digits=10, decimal_places=6, null=True)
    longitude = models.DecimalField(max_digits=10, decimal_places=6, null=True)
    accuracy_radius = models.PositiveSmallIntegerField(null=True)

    updated = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "thl_ipinformation"
        indexes = [
            models.Index(fields=["updated"]),
        ]


class GeoName(models.Model):
    """
    Stores information about the city, continent, country, postal, and subdivisions
    https://dev.maxmind.com/geoip/docs/databases/city-and-country#locations-files

    All of this info comes back in an Insights API call. We can check if the row
        exists in this table, and just create it once.
    """

    geoname_id = models.PositiveIntegerField(primary_key=True, null=False)
    # We could store place names in other languages, but I don't anticipate us
    #   doing this. If we did, the primary key would be (geoname_id, locale_code).
    # locale_code = models.CharField(max_length=5, default='eng')

    # AF - Africa, AN - Antarctica, AS - Asia, EU - Europe,
    # NA - North America, OC - Oceania, SA - South America
    continent_code = models.CharField(max_length=2, blank=False, null=False)
    continent_name = models.CharField(max_length=32, blank=False, null=False)

    # Below here are all optional, although country will be set 99% of the time
    country_iso = models.CharField(max_length=2, blank=False, null=True)
    country_name = models.CharField(max_length=64, blank=False, null=True)
    subdivision_1_iso = models.CharField(max_length=3, blank=False, null=True)
    subdivision_1_name = models.CharField(max_length=255, blank=False, null=True)
    subdivision_2_iso = models.CharField(max_length=3, blank=False, null=True)
    subdivision_2_name = models.CharField(max_length=255, blank=False, null=True)
    city_name = models.CharField(max_length=255, blank=False, null=True)
    metro_code = models.PositiveSmallIntegerField(null=True)
    time_zone = models.CharField(max_length=60, blank=False, null=True)
    is_in_european_union = models.BooleanField(null=True)

    updated = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "thl_geoname"
        indexes = [
            models.Index(fields=["updated"]),
        ]


class LedgerDirection(models.IntegerChoices):
    # This choice of Positive/Negative per direction is arbitrary and is
    #   just used for ease of multiplying numbers together later instead of
    #   using strings.
    CREDIT = -1, "credit"
    DEBIT = 1, "debit"


class LedgerAccount(models.Model):
    """
    A ledger_account is an account in a double-entry accounting system. Each
        ledger account can optionally be associated with a uuid in another
        table, such as a brokerage product or user.

    Further reading: https://docs.moderntreasury.com/ledgers/docs/digital-wallet-tutorial?tab=Transactions-API
    """

    uuid = models.UUIDField(null=False, primary_key=True)

    # Name which could be used to display this account
    display_name = models.CharField(max_length=64, null=False)

    # A fully qualified name which could be used for the purposes of grouping
    #   and placing this account into a hierarchical structure. The elements
    #   are colon-separated. The fully qualified name must be unique. This
    #   could be used to look up an account.
    qualified_name = models.CharField(max_length=255, null=False, unique=True)

    # Used to tag an account with its general "purpose". Could be used for
    #   group bys for reporting purposes.
    #   e.g. "bp_commission" which stores commission from BP payments
    account_type = models.CharField(max_length=30, null=True)

    # Each account must be debit or credit-normal.
    normal_balance = models.SmallIntegerField(
        null=False, choices=LedgerDirection.choices
    )

    # Could be a reference to a BP, or account, or user in another table.
    reference_type = models.CharField(max_length=30, null=True)
    reference_uuid = models.UUIDField(null=True)

    # The currency for this account's transactions. For now, all will be "USD".
    #   I can imagine we could have a LedgerCurrency table that store the:
    #   currency_exponent, display format str, conversion_rate, etc, and then
    #   this would be a uuid/pk into that table.
    currency = models.CharField(max_length=32, null=False)

    # The currency's smallest denomination unit. For e.g. an account
    #   denomination in USD has a currency_exponent of 2, because 1 cent =
    #   1*10^-2 USD. I think this is just used for display purposes.
    # currency_exponent = models.SmallIntegerField(null=False)

    class Meta:
        db_table = "ledger_account"
        indexes = [
            # This is not a unique index because an entity could have
            #   multiple accounts
            #
            # I don't think we need an index on reference_type b/c no two
            #   entities should have the same uuids anyway.
            models.Index(fields=["reference_uuid"]),
        ]


class LedgerTransaction(models.Model):
    """
    A ledger_transaction is a transaction between two or more ledger accounts.
        To create a ledger transaction, there must be at least one credit
        ledger entry and one debit ledger entry. Additionally, the sum of all
        credit entry amounts must equal the sum of all debit entry amounts.
    """

    id = models.BigAutoField(primary_key=True, null=False)
    created = models.DateTimeField(null=False)

    # Optionally add notes to the transaction that could be displayed in
    # an account statement
    ext_description = models.CharField(max_length=255, null=True)

    # Optionally tag a transaction for quick and easy searching (used for
    # de-duplication / locking purposes)
    tag = models.CharField(max_length=255, null=True)

    class Meta:
        db_table = "ledger_transaction"
        indexes = [
            models.Index(fields=["created"]),
            models.Index(fields=["tag"]),
        ]


class LedgerTransactionMetadata(models.Model):
    """
    Used to associate a transaction with metadata: a thl_session, or thl_wall,
        or user quality history event, or multiple of each, or something
        else in the future ...
    """

    id = models.BigAutoField(primary_key=True, null=False)
    transaction = models.ForeignKey(
        LedgerTransaction, on_delete=models.RESTRICT, null=False
    )
    key = models.CharField(max_length=30)
    value = models.CharField(max_length=255)

    class Meta:
        db_table = "ledger_transactionmetadata"
        # You can only have 1 key per transaction. So a transaction cannot
        #   be associated with multiple thl_session uuids for e.g., but it
        #   can be associated with a thl_session and uqh.
        #
        # If there is a need to associate a transaction with a list of
        #   thl_sessions (for example a bonus for 10 completes), the
        #   transaction should instead be associated with a single "contest"
        #   object that itself points to those 10 completes (or whatever).
        unique_together = ("transaction", "key")
        indexes = [
            models.Index(fields=["key", "value"]),
        ]


class LedgerEntry(models.Model):
    """
    A.K.K "line item". A ledger_entry represents an accounting entry within
        a parent ledger transaction.
    """

    id = models.BigAutoField(primary_key=True, null=False)
    direction = models.SmallIntegerField(null=False, choices=LedgerDirection.choices)
    account = models.ForeignKey(
        LedgerAccount,
        on_delete=models.RESTRICT,
        null=False,
        related_name="account",
    )
    # In the smallest unit of the currency being transacted. For
    #   USD, this is cents.
    amount = models.BigIntegerField(null=False)
    transaction = models.ForeignKey(
        LedgerTransaction,
        on_delete=models.RESTRICT,
        null=False,
        related_name="transaction",
    )

    class Meta:
        db_table = "ledger_entry"


class LedgerAccountStatement(models.Model):
    """
    Provides the starting and ending balances of a ledger account for a
        specific time period. The statement could optionally apply a metadata
        filter to the account.
    """

    id = models.BigAutoField(primary_key=True, null=False)
    account = models.ForeignKey(LedgerAccount, on_delete=models.RESTRICT, null=False)

    # For optional filtering: key/values applied to the
    #   LedgerTransactionMetadata to filter transactions for this account, as
    #   a '&' delimited, key=value string (sorted by key). e.g.
    #   "transaction_type=cashout&user_id=12345"
    filter_str = models.CharField(max_length=255, null=True)

    # The inclusive lower bound of the effective_at timestamp of the ledger
    #   entries to be included in the statement
    effective_at_lower_bound = models.DateTimeField(null=False)

    # The exclusive upper bound of the effective_at timestamp of the ledger
    #   entries to be included in the statement
    effective_at_upper_bound = models.DateTimeField(null=False)
    starting_balance = models.BigIntegerField(null=False)
    ending_balance = models.BigIntegerField(null=False)

    # sql query used to generate this data
    sql_query = models.TextField(null=True)

    class Meta:
        db_table = "ledger_accountstatement"
        indexes = [
            models.Index(fields=["account", "filter_str", "effective_at_lower_bound"]),
        ]
        # Maybe there should be a unique index on ('account', 'filter_str',
        #   'effective_at_lower_bound', 'effective_at_upper_bound') ?
        #
        # Maybe we should add a OPEN/CLOSED flag to indicate the time period
        #   is still "open", and another field with the timestamp of the last
        #   transaction within the statements, so that we can
        #   continuously update a statement.


class TaskAdjustment(models.Model):
    """
    This used to be userprofile.UserQualityHistory. This now only stores
        Task Adjustments/recons. Any other quality types should go in he
        userhealth_auditlog.

    This stores a reference to a THLWall record (wall_uuid), which should
        have identical source, survey_id, and started values (copied here
        for convenience).
    """

    uuid = models.UUIDField(default=uuid.uuid4, primary_key=True)

    # This is 'af' (adjusted to fail), 'ac' (adjusted to complete), 'cc'
    #   (confirmed complete),  or possibly 'ca' (cpi adjustment) (not yet
    #   supported).
    adjusted_status = models.CharField(max_length=2, null=False)

    # External status code: marketplace's status / status code / status
    #   reason / whatever they call it.
    ext_status_code = models.CharField(max_length=32, null=True)

    # The amount that is being adjusted. If positive, this is the amount
    #   added to the original payment, if negative, this amount is taken
    #   back (complete -> recon). This should agree with adjusted_status.
    #
    # This should be NULL only if the adjusted_status is cc.
    #
    # Note: this is in USD b/c THLWall cpi and adjusted_cpi are in USD, and
    #   we only ever transact in task completions in USD.
    amount = models.DecimalField(decimal_places=2, max_digits=5, null=True)

    # When were we notified about this?
    alerted = models.DateTimeField(null=False)

    # When we created this record.
    created = models.DateTimeField(auto_now_add=True)

    # This is inferrable through the wall_uuid -> thl_session, but copied
    #   here for convenience
    user_id = models.BigIntegerField(null=False)

    # This is the wall event that had the adjustment
    wall_uuid = models.UUIDField(null=False)

    # These 3 are also inferrable through thl_wall, but copied here for
    # convenience. When the user started the task that had a quality event
    started = models.DateTimeField(null=False)
    source = models.CharField(max_length=2, null=False)
    survey_id = models.CharField(max_length=32, null=False)

    class Meta:
        db_table = "thl_taskadjustment"

        indexes = [
            models.Index(fields=["created"]),
            models.Index(fields=["user_id"]),
            models.Index(fields=["wall_uuid"]),
        ]
