import uuid

from django.db import models


class Bribe(models.Model):
    """
    This is meant to store info about "manual" bribes, in which Customer
        Support directly gives a user money (put into their wallet) as a result
        of an email/support communication.

    Each bribe would have its own row, with the metadata about the bribe in
        the data field.

    There is no "sent" field, because the financial impact of bribes is
        determined by the ledger, not by presence or absence in this table.
    """

    uuid = models.UUIDField(default=uuid.uuid4, primary_key=True)

    # This is the LedgerAccount.uuid that this Payout Event is associated with.
    #   The user/BP is retrievable through the LedgerAccount.reference_uuid.
    credit_account_uuid = models.UUIDField(null=False)
    created = models.DateTimeField(auto_now_add=True)

    # In the smallest unit of the currency being transacted. For USD, this
    #   is cents.
    amount = models.BigIntegerField(null=False)
    ext_ref_id = models.CharField(max_length=64, null=True)  # support ticket ID?
    description = models.TextField(
        null=True
    )  # could be shown to the user in their transactions description
    data = models.JSONField(null=True)  # content of email? (optional)

    class Meta:
        db_table = "event_bribe"

        indexes = [
            models.Index(fields=["created"]),
            models.Index(fields=["credit_account_uuid"]),
            models.Index(fields=["ext_ref_id"]),
        ]


class Payout(models.Model):
    """
    Money is paid out of a virtual wallet.
    """

    uuid = models.UUIDField(default=uuid.uuid4, primary_key=True)

    # This is the LedgerAccount.uuid that this money is being requested from.
    #   The user/BP is retrievable through the LedgerAccount.reference_uuid
    debit_account_uuid = models.UUIDField(null=False)

    # References a row in the account_cashoutmethod table. This is the cashout
    #   method that was used to request this payout. (A cashout is the same
    #   thing as a payout)
    cashout_method_uuid = models.UUIDField(null=False)
    created = models.DateTimeField(auto_now_add=True)

    # In the smallest unit of the currency being transacted. For USD, this is cents.
    amount = models.BigIntegerField(null=False)

    # The allowed values for `status` are defined in py-utils:
    #   generalresearch/models/thl/payout.py:PayoutStatus
    status = models.CharField(max_length=20, null=True)

    # Used for holding an external, payouttype-specific identifier
    ext_ref_id = models.CharField(max_length=64, null=True)

    # The allowed values for `payout_type` are defined in py-utils:
    #   generalresearch/models/thl/payout.py:PayoutType
    payout_type = models.CharField(max_length=14)

    # Stores payout-type-specific information that is used to request this
    #   payout from the external provider.
    request_data = models.JSONField(null=True)

    # Stores payout-type-specific order information that is returned from
    #   the external payout provider.
    order_data = models.JSONField(null=True)

    class Meta:
        db_table = "event_payout"

        indexes = [
            models.Index(fields=["created"]),
            models.Index(fields=["debit_account_uuid"]),
            models.Index(fields=["ext_ref_id"]),
        ]
