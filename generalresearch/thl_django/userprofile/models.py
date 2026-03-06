import uuid

from django.db import models


class UserStat(models.Model):
    """This is for storing userstats calculated by yieldman. Only one user_id,
    key is allowed and the value gets updated.
    """

    user_id = models.PositiveIntegerField()
    key = models.CharField(max_length=255)
    value = models.FloatField(null=True)
    date = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "userprofile_userstat"

        unique_together = ("key", "user_id")
        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["user_id"]),
        ]


class BrokerageProduct(models.Model):
    """Represents a FSB, or other Product on General Research"""

    id = models.UUIDField(primary_key=True)
    id_int = models.BigIntegerField(null=False, unique=True)

    name = models.CharField(max_length=255, unique=False)
    # For migration, then change to false
    team_id = models.UUIDField(null=True)
    business_id = models.UUIDField(null=True)

    # We can back-pop the created timestamps from GR
    created = models.DateTimeField(auto_now_add=True, null=True)

    enabled = models.BooleanField(default=True)
    payments_enabled = models.BooleanField(default=True)

    # --- Config fields (some of these used to be in BrokerageProductConfig) ---

    # The commission percentage we charge. Should be between 0 and 1
    #   inclusive. Temporarily null=True
    commission = models.DecimalField(null=True, decimal_places=6, max_digits=6)

    # Where users are redirected to after finishing a task. Formerly known
    #   as callback_uri. This is temporarily null=True.
    redirect_url = models.URLField(null=True)

    # The domain to use for GRS. Formerly known as harmonizer_domain. This is
    #   temporarily null=True.
    grs_domain = models.CharField(max_length=200, null=True)

    # Stores config for the Profiling experience. FKA harmonizer_config.
    #   (e.g. task_injection_freq_mult, n_questions)
    profiling_config = models.JSONField(default=dict)

    # Stores config for UserHealth (e.g. allow_ban_iphist, conversion_cutoff)
    user_health_config = models.JSONField(default=dict)

    # Stores config for yield management (e.g. conversion_factor_adj). These
    #   are things that pertain to single tasks.
    yield_man_config = models.JSONField(default=dict)

    # Stores config for offerwall creation (e.g. min_bin_size, n_bins)
    offerwall_config = models.JSONField(default=dict)

    # Stores config for session creation (e.g. max_session_len,
    #   max_session_hard_retry, min_payout, etc)
    session_config = models.JSONField(default=dict)

    # Store config for payouts and user payouts (payout_transformation,
    #   payout_format, etc.)
    payout_config = models.JSONField(default=dict)

    # Store configuration regarding user creation. See: models/thl/product.py:UserCreateConfig
    user_create_config = models.JSONField(default=dict)

    class Meta:
        db_table = "userprofile_brokerageproduct"

        # Each name has to be unique within a team, but there can be multiple
        #   BPs with the same name overall
        unique_together = ("team_id", "name")


class BrokerageProductConfig(models.Model):
    """
    Represents the configuration settings for a FSB, or other Product
    on General Research
    """

    product = models.ForeignKey(
        BrokerageProduct, null=False, on_delete=models.DO_NOTHING
    )
    key = models.CharField(max_length=255)
    value = models.JSONField(default=dict)

    class Meta:
        db_table = "userprofile_brokerageproductconfig"

        unique_together = ("product", "key")


class BrokerageProductTag(models.Model):
    """
    Stores Tags for brokerage products which can be used to annotate
    supplier traffic
    """

    id = models.BigAutoField(primary_key=True, null=False)

    product_id = models.BigIntegerField(null=False)

    # The allowed values are defined in models/thl/supplier_tag.py
    tag = models.CharField(max_length=64, null=False)

    class Meta:
        db_table = "userprofile_brokerageproducttag"

        # Tags are unique per product
        unique_together = ("product_id", "tag")


class Language(models.Model):
    """
    Languages we allow user's to do tasks in
    Uses the ISO 639-2/B system.

    https://en.wikipedia.org/wiki/List_of_ISO_639-2_codes
    """

    code = models.CharField(
        primary_key=True, max_length=3, help_text="three-letter language code"
    )
    name = models.CharField(max_length=255, help_text="language name")

    class Meta:
        db_table = "userprofile_language"


class PayoutMethod(models.Model):
    """

    ***Deprecated*** Nothing uses this

    An "Account" for users to send money to. Separated out as it
    shouldn't be tied to authentication, and a user might want to
    send to multiple places
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user_id = models.IntegerField(null=True, db_index=True)

    default = models.BooleanField(default=False)
    # We'll never delete these, so need a way to monitor
    enabled = models.BooleanField(default=True)

    PAYOUT_CHOICES = (
        ("a", "AMT"),
        ("c", "ACH"),
        ("t", "Tango"),
        ("p", "PAYPAL"),
    )
    method = models.CharField(choices=PAYOUT_CHOICES, max_length=1, default="t")
    recipient = models.CharField(max_length=200, blank=True, null=True)

    updated = models.DateTimeField(auto_now=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "userprofile_payoutmethod"

        ordering = ("-created",)
        get_latest_by = "created"
