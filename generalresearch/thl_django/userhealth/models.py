from django.db import models


class UserHealthIPHistory(models.Model):
    user_id = models.BigIntegerField(null=False)
    ip = models.GenericIPAddressField()
    created = models.DateTimeField(auto_now_add=True)
    # Store any IPs in the X-Forwarded-For header, in order starting
    #   with forwarded_ip1
    forwarded_ip1 = models.GenericIPAddressField(null=True)
    forwarded_ip2 = models.GenericIPAddressField(null=True)
    forwarded_ip3 = models.GenericIPAddressField(null=True)
    forwarded_ip4 = models.GenericIPAddressField(null=True)
    forwarded_ip5 = models.GenericIPAddressField(null=True)
    forwarded_ip6 = models.GenericIPAddressField(null=True)

    class Meta:
        """
        We should NOT have a unique index on ('user_id', 'ip') b/c we should
        insert a duplicate row (w a new timestamp) if this user is still using
        this IP after N days (~7?). So that we never have to look too far back
        to get a user's "current" IP.
        """

        db_table = "userhealth_iphistory"
        indexes = [
            models.Index(fields=["user_id", "created"]),
            models.Index(fields=["created"]),
            models.Index(fields=["ip"]),
        ]


class UserHealthWebSocketIPHistory(models.Model):
    """
    Table for logging any websocket request that came from our GRS page.

    field:last_seen - the latest timestamp of user's particular IP address
        that he hit us with before he switched IP address (or before NOW)

    Example using user_id,IP,timestamp:
        12345, x.x.x.x, 2023-11-11 16:00
        12345, x.x.x.x, 2023-11-11 16:02
        12345, x.x.x.x, 2023-11-11 16:03
        12345, y.y.y.y, 2023-11-11 16:05
        12345, x.x.x.x, 2023-11-11 16:07
        98765, x.x.x.x, 2023-11-11 16:08
        12345, z.z.z.z, 2023-11-11 16:10
        12345, y.y.y.y, 2023-11-11 16:12
        12345, y.y.y.y, 2024-11-11 16:15

    Then Mysql data:
        user_id, IP, created, last_seen
        12345, x.x.x.x, 2023-11-11 16:00, 2023-11-11 16:03
        12345, y.y.y.y, 2023-11-11 16:05, 2023-11-11 16:05
        12345, x.x.x.x, 2023-11-11 16:07, 2023-11-11 16:07
        98765, x.x.x.x, 2023-11-11 16:08, 2023-11-11 16:08
        12345, z.z.z.z, 2023-11-11 16:10, 2023-11-11 16:10
        12345, y.y.y.y, 2023-11-11 16:12, 2024-11-11 16:15
    """

    user_id = models.BigIntegerField(null=False)
    ip = models.GenericIPAddressField()
    created = models.DateTimeField(auto_now_add=True)
    last_seen = models.DateTimeField(auto_now_add=True)

    class Meta:
        """
        I'm not sure about the use case of index (user_id, last_seen). But
        inserting data in this table is not in the hot path, so let's keep it.
        """

        db_table = "userhealth_iphistory_ws"
        indexes = [
            models.Index(fields=["user_id", "created"]),
            models.Index(fields=["user_id", "last_seen"]),
            models.Index(fields=["created"]),
            models.Index(fields=["last_seen"]),
            models.Index(fields=["ip"]),
        ]


class UserAuditLog(models.Model):
    """
    Table for logging "actions" taken by a user or "events" related to a user
    """

    # The table will have a default autoincrement key
    id = models.BigAutoField(primary_key=True)

    # The user this event pertains to
    user_id = models.BigIntegerField(null=False)

    # When this event happened
    created = models.DateTimeField(null=False)

    # The level of importance for this event. Works the same as python
    #   logging levels. It is an integer 0 - 50, and implementers of this
    #   field could map it to the predefined levels: (CRITICAL, ERROR, WARNING,
    #   INFO, DEBUG).
    #
    # This is NOT the same concept as the "strength" of whatever event happened;
    #   it is just for sorting, filtering and display purposes. For e.g.
    #   multiple level 20 events != the "importance" of one level 40 event.
    level = models.PositiveSmallIntegerField(null=False, default=0)

    # The "class" or "type" or event that happened.
    # e.g. "upk-audit", "ip-audit", "entrance-limit"
    event_type = models.CharField(max_length=64, null=False)

    # The event message. Could be displayed on user's page
    event_msg = models.CharField(max_length=256, null=True)

    # Optionally store a numeric value associated with this event. For e.g.
    #   if we recalculate the user's normalized recon rate, and it is "high",
    #   we could store an event like (event_type="recon-rate",
    #   event_msg="higher than allowed recon rate", event_value=0.42)
    event_value = models.FloatField(null=True)

    class Meta:
        db_table = "userhealth_auditlog"

        indexes = [
            models.Index(fields=["created"]),
            models.Index(fields=["user_id", "created"]),
            models.Index(fields=["level", "created"]),
            models.Index(fields=["event_type", "created"]),
        ]
