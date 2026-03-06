from django.db import models


class Contest(models.Model):
    """ """

    id = models.BigAutoField(primary_key=True, null=False)

    uuid = models.UUIDField(null=False, unique=True)
    product_id = models.UUIDField(null=False)

    name = models.CharField(max_length=128)
    description = models.CharField(max_length=2048, null=True)
    country_isos = models.CharField(max_length=1024, null=True)

    contest_type = models.CharField(max_length=32)
    status = models.CharField(max_length=32)

    starts_at = models.DateTimeField()
    terms_and_conditions = models.CharField(max_length=2048, null=True)

    end_condition = models.JSONField()
    prizes = models.JSONField()

    # ---- Only set when the contest ends ----
    ended_at = models.DateTimeField(null=True)
    end_reason = models.CharField(max_length=32, null=True)
    # ---- END Only set when the contest ends ----

    # ---- Contest-type-specific keys ----

    # For raffle contests
    entry_type = models.CharField(max_length=8, null=True)
    entry_rule = models.JSONField(null=True)

    # These get calculated by / (are dependent on) the entries, but I'm adding
    #   these as fields, so we can quickly retrieve them without having to join
    #   on the entry table and redo the summations.
    # They are nullable because they do not apply to leaderboard contests
    current_participants = models.IntegerField(null=True)
    current_amount = models.IntegerField(null=True)

    # For Milestone
    milestone_config = models.JSONField(null=True)
    # For keeping track of the number of times this milestone has been reached
    win_count = models.IntegerField(null=True)

    # For LeaderboardContest
    # e.g. 'leaderboard:48d6ff6664bc4767a0d8e5381f7e5cf0:us:monthly:2024-01-01:largest_user_payout'
    leaderboard_key = models.CharField(max_length=128, null=True)

    # ---- END Contest-type-specific keys ----

    created_at = models.DateTimeField(auto_now_add=True, null=False)
    # updated_at gets set to created_at when object is created!
    # updated_at means a property of the contest itself is modified, NOT
    #   including an entry being created/modified.
    updated_at = models.DateTimeField(auto_now=True, null=False)

    class Meta:
        db_table = "contest_contest"
        indexes = [
            # id and uuid will already have an index
            models.Index(fields=["product_id", "created_at"]),
            models.Index(fields=["product_id", "status"]),
        ]


class ContestEntry(models.Model):
    id = models.BigAutoField(primary_key=True, null=False)
    uuid = models.UUIDField(null=False, unique=True)

    # The Contest.id this entry pertains to
    contest_id = models.BigIntegerField(null=False)

    amount = models.IntegerField(null=False)
    user_id = models.BigIntegerField(null=False)

    created_at = models.DateTimeField(auto_now_add=True, null=False)
    # updated_at gets set to created_at when object is created!
    # Raffle entries are NOT modifiable, but in a milestone contest,
    #   we'll update the 'amount' per (contest, user).
    updated_at = models.DateTimeField(auto_now=True, null=False)

    class Meta:
        db_table = "contest_contestentry"
        indexes = [
            # id and uuid will already have an index
            models.Index(fields=["user_id", "created_at"]),
            models.Index(fields=["contest_id", "user_id"]),
        ]


class ContestWinner(models.Model):
    id = models.BigAutoField(primary_key=True, null=False)
    uuid = models.UUIDField(null=False, unique=True)

    # The Contest.id this entry pertains to
    contest_id = models.BigIntegerField(null=False)
    user_id = models.BigIntegerField(null=False)

    prize = models.JSONField(null=False)
    # If it's a tie, and the prize is cash, multiple users may split a prize.
    awarded_cash_amount = models.IntegerField(null=True)

    # Milestone winners are created at different times, so we need this also.
    created_at = models.DateTimeField(auto_now_add=True, null=False)

    class Meta:
        db_table = "contest_contestwinner"
        indexes = [
            # id and uuid will already have an index
            models.Index(fields=["user_id", "created_at"]),
            models.Index(fields=["contest_id"]),
        ]
