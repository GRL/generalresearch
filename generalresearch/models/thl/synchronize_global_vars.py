from typing import List

from pydantic import Field, BaseModel


class SynchronizeGlobalVarsMsg(BaseModel):
    """Used within a Redis pub/sub to clear/invalidate internal caches,
    typically on objects stored in GLOBAL_VARS or functools caches.
    """

    # Specifies the key / subkey to be acted upon.
    #  For example ["mrpq", 123] would apply to GLOBAL_VARS["mrpq"][123]
    key_path: List[str] = Field()
    # e.g. GLOBAL_VARS["mrpq"].pop(123, None)
    pop: bool = Field(default=False)
    # e.g. GLOBAL_VARS["mrpq"][123].clear()
    clear: bool = Field(default=False)
