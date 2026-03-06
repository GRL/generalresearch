from typing import Literal

from generalresearch.incite.collections import DFCollection, DFCollectionType


class UserDFCollection(DFCollection):
    data_type: Literal[DFCollectionType.USER] = DFCollectionType.USER


class WallDFCollection(DFCollection):
    data_type: Literal[DFCollectionType.WALL] = DFCollectionType.WALL


class SessionDFCollection(DFCollection):
    data_type: Literal[DFCollectionType.SESSION] = DFCollectionType.SESSION


class IPInfoDFCollection(DFCollection):
    data_type: Literal[DFCollectionType.IP_INFO] = DFCollectionType.IP_INFO


class IPHistoryDFCollection(DFCollection):
    data_type: Literal[DFCollectionType.IP_HISTORY] = DFCollectionType.IP_HISTORY


class IPHistoryWSDFCollection(DFCollection):
    data_type: Literal[DFCollectionType.IP_HISTORY_WS] = DFCollectionType.IP_HISTORY_WS


class TaskAdjustmentDFCollection(DFCollection):
    data_type: Literal[DFCollectionType.TASK_ADJUSTMENT] = (
        DFCollectionType.TASK_ADJUSTMENT
    )


class AuditLogDFCollection(DFCollection):
    data_type: Literal[DFCollectionType.AUDIT_LOG] = DFCollectionType.AUDIT_LOG


class LedgerDFCollection(DFCollection):
    data_type: Literal[DFCollectionType.LEDGER] = DFCollectionType.LEDGER
