import json
from typing import Dict

from generalresearch.models.network.mtr.result import MTRResult


def parse_mtr_output(raw: str, port, protocol) -> MTRResult:
    data = parse_mtr_raw_output(raw)
    data["port"] = port
    data["protocol"] = protocol
    return MTRResult.model_validate(data)


def parse_mtr_raw_output(raw: str) -> Dict:
    data = json.loads(raw)["report"]
    data.update(data.pop("mtr"))
    data["hops"] = data.pop("hubs")
    return data
