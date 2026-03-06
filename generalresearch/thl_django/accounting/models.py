import uuid

from django.db import models


class CashoutMethod(models.Model):
    """
    Stores info about different methods a user could use to redeem money
    from their wallet. Each entry is a specific instance of a "thing" a user
    can get, for e.g. a Visa Prepaid Card from Tango, and there will be many
    CashoutMethods, all of provider "tango".
    """

    # The primary identifier for this method, may be exposed external to THL
    id = models.UUIDField(default=uuid.uuid4, primary_key=True)
    last_updated = models.DateTimeField(auto_now=True)
    is_live = models.BooleanField(default=False)

    # This is the service that is "handling" the cashout, e.g. "TANGO",
    # "DWOLLA", "PAYPAL", etc...
    provider = models.CharField(max_length=32)

    # This is the method_provider's identifier for this cashout method
    # (e.g. if method_provider = TANGO, this is the UTID. A (method_provider,
    # ext_id) should uniquely map to an `id`.
    ext_id = models.CharField(max_length=255, null=True)

    # Not required here as it will prob be in `data`, but just for convenience.
    name = models.CharField(max_length=512)

    # Other method_class-specific data (min_value, max_value, value_type,
    # disclaimer, etc...)
    data = models.JSONField(default=dict)

    # For creating user-specific cashout methods
    user_id = models.PositiveIntegerField(null=True)

    class Meta:
        db_table = "accounting_cashoutmethod"

        indexes = [
            models.Index(fields=["user_id"]),
            models.Index(fields=["provider", "ext_id"]),
        ]
