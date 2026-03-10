from django.db import models
import ipaddress


class CIDRField(models.Field):
    description = "PostgreSQL CIDR network"

    def db_type(self, connection):
        return "cidr"

    def to_python(self, value):
        if value is None or isinstance(
            value, (ipaddress.IPv4Network, ipaddress.IPv6Network)
        ):
            return value
        return ipaddress.ip_network(value, strict=False)

    def get_prep_value(self, value):
        if value is None:
            return None
        return str(ipaddress.ip_network(value, strict=False))
