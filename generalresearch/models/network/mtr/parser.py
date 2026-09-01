import json
from typing import TYPE_CHECKING

from generalresearch.models.network.mtr.result import MTRResult

if TYPE_CHECKING:
    from generalresearch.models.network.definitions import IPProtocol


def parse_mtr_output(raw: str, port: int, protocol: IPProtocol) -> MTRResult:
    data = parse_mtr_raw_output(raw)
    data["port"] = port
    data["protocol"] = protocol
    return MTRResult.model_validate(data)


def parse_mtr_raw_output(raw: str) -> dict:
    data = json.loads(raw)["report"]
    data.update(data.pop("mtr"))
    data["hops"] = data.pop("hubs")
    return data
