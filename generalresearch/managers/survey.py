from abc import ABC
from typing import List

from generalresearch.managers.base import SqlManager
from generalresearch.models.thl.survey import MarketplaceTask


class SurveyManager(SqlManager, ABC):

    def create(self, survey: MarketplaceTask) -> bool:
        """
        Create a single survey
        """
        ...

    def update(self, surveys: List[MarketplaceTask]) -> bool:
        """
        Update a list of surveys. Depending on the implementation, this may
          operate one by one or as a bulk update.
        """
        ...

    def update_field(self, survey: MarketplaceTask, field: str) -> bool:
        """
        Update only `field` from `survey`. The survey must already exist. We expect
          that you've already checked that the field's value is different.
        """
