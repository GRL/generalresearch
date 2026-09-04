from pydantic import BaseModel, PositiveInt

from generalresearch.models.custom_types import UUIDStr
from generalresearch.models.thl.user_identifiers import BPUIDStr


class UserRef(BaseModel):
    """
    Use in place of the full User model in places where we want to
    associate something with a User, but can't use the full User
    model due to cyclic import issues.
    As a side-effect, this also avoids the type|None ruff issues.
    """

    user_id: PositiveInt
    product_id: UUIDStr
    product_user_id: BPUIDStr
