import json
from functools import cached_property
from typing import Optional, List

import tldextract
from pydantic import BaseModel, Field, model_validator, computed_field

from generalresearch.models.custom_types import IPvAnyAddressStr


class RDNSResult(BaseModel):

    ip: IPvAnyAddressStr = Field()

    hostnames: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_hostname_prop(self):
        assert len(self.hostnames) == self.hostname_count
        if self.hostnames:
            assert self.hostnames[0] == self.primary_hostname
            assert self.primary_domain in self.primary_hostname
        return self

    @computed_field(examples=["fixed-187-191-8-145.totalplay.net"])
    @cached_property
    def primary_hostname(self) -> Optional[str]:
        if self.hostnames:
            return self.hostnames[0]

    @computed_field(examples=[1])
    @cached_property
    def hostname_count(self) -> int:
        return len(self.hostnames)

    @computed_field(examples=["totalplay.net"])
    @cached_property
    def primary_domain(self) -> Optional[str]:
        if self.primary_hostname:
            return tldextract.extract(
                self.primary_hostname
            ).top_domain_under_public_suffix

    def model_dump_postgres(self):
        # Writes for the network_rdnsresult table
        d = self.model_dump(
            mode="json",
            include={"primary_hostname", "primary_domain", "hostname_count"},
        )
        d["hostnames"] = json.dumps(self.hostnames)
        return d
