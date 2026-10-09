import uuid

from django.db import models


class User(models.Model):
    id = models.BigAutoField(primary_key=True)
    sub = models.CharField(max_length=200, blank=False, null=False)

    is_superuser = models.BooleanField(default=False)
    date_joined = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "gr_user"


class UserToken(models.Model):
    key = models.CharField(max_length=255, primary_key=True)
    user_id = models.BigIntegerField(null=True, db_index=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "gr_token"


class Membership(models.Model):
    """
    A User on generalresearch.com can be a member of many teams, that relationship can have different
    permission to manage a team or accounts and business entities owned by that team
    """

    id = models.BigAutoField(primary_key=True)
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)

    user_id = models.BigIntegerField(null=True, db_index=True)
    team_id = models.BigIntegerField(null=True, db_index=True)

    PRIVILEGES = (
        (0, "Admin"),
        (1, "Maintain"),
        (2, "Read"),
    )
    privilege = models.PositiveSmallIntegerField(default=1, choices=PRIVILEGES)
    owner = models.BooleanField(default=False)

    created = models.DateTimeField(auto_now_add=True, null=False)


class Team(models.Model):
    """
    This is a collection of users / accounts that work as a team and share
    access to the same BPID  However, an Organization can have many Bank
    Accounts as each BPID doesn't need to deposit funds into the same Bank
    account or Business
    """

    id = models.BigAutoField(primary_key=True)
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)

    name = models.CharField(max_length=255, null=True, blank=True)

    businesses = models.ManyToManyField(to="common.Business")


class Business(models.Model):
    """
    A Team can manage multiple Businesses
    A Business can have multiple Brokerage Products

        # The IRS responsible entity that our accounts care about for issuing 1099s.
        # We have this seperated from the Team as some Teams have different
        # bank accounts + corporate entities for the activity of an app
    """

    id = models.BigAutoField(primary_key=True)
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)

    BUSINESS_KINDS = (("i", "Individual"), ("c", "Company"))
    kind = models.CharField(max_length=1, choices=BUSINESS_KINDS, default="i")
    name = models.CharField(max_length=255, blank=True, null=False)

    tax_number = models.CharField(max_length=20, blank=True, null=True)

    # BusinessBalances model
    balance = models.JSONField(default=None, null=True)
    # BusinessUserWalletBalances model
    user_wallet_balance = models.JSONField(default=None, null=True)

    users_active_7d = models.IntegerField(default=None, null=True)
    task_completes_7d = models.IntegerField(default=None, null=True)
    # Net Earnings over the last 7 days (in USD Cents, this can be positive or negative)
    balance_net_7d = models.IntegerField(default=None, null=True)


class BusinessAddress(models.Model):
    id = models.BigAutoField(primary_key=True)
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)

    business = models.ForeignKey(
        "common.Business",
        null=True,
        on_delete=models.SET_NULL,
    )

    # Location Details
    line_1 = models.CharField(max_length=255, blank=True, null=True)
    line_2 = models.CharField(max_length=255, blank=True, null=True)
    city = models.CharField(max_length=255, blank=True, null=True)
    country = models.CharField(
        max_length=2, blank=True, null=True
    )  # Validate this with pycountry
    state = models.CharField(max_length=255, blank=True, null=True)
    postal_code = models.CharField(max_length=12, blank=True, null=True)
    phone_number = models.CharField(max_length=20, null=True)


class BankAccount(models.Model):
    """
    A Business can have a single active Bank Account at the time.
    This is used to deposit funds earned from the Business's Brokerage Products.
    """

    id = models.BigAutoField(primary_key=True)
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)

    business = models.ForeignKey(
        "common.Business",
        null=True,
        on_delete=models.SET_NULL,
    )

    TRANSFER_METHOD = (
        (0, "ACH"),
        (1, "Wire"),
    )
    transfer_method = models.PositiveSmallIntegerField(
        choices=TRANSFER_METHOD, default=0
    )

    # ACH requirements
    account_number = models.CharField(max_length=50, blank=True, null=True)
    routing_number = models.CharField(max_length=50, blank=True, null=True)

    # Wire requirements
    iban = models.CharField(max_length=50, blank=True, null=True)
    swift = models.CharField(max_length=50, blank=True, null=True)
