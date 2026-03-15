pytest_plugins = [
    "distributed.utils_test",
    "test_utils.conftest",
    # -- GRL IQ
    "test_utils.grliq.conftest",
    "test_utils.grliq.managers.conftest",
    "test_utils.grliq.models.conftest",
    # -- Incite
    "test_utils.incite.conftest",
    "test_utils.incite.collections.conftest",
    "test_utils.incite.mergers.conftest",
    # -- Managers
    "test_utils.managers.conftest",
    "test_utils.managers.contest.conftest",
    "test_utils.managers.ledger.conftest",
    "test_utils.managers.network.conftest",
    "test_utils.managers.upk.conftest",
    # -- Models
    "test_utils.models.conftest",
]
