pytest_plugins = [
    "distributed.utils_test",
    "test_utils.conftest",
    # -- GRL IQ
    "test_utils.grliq.conftest",
    # -- Incite
    "test_utils.incite.conftest",
    "test_utils.incite.collections.conftest",
    "test_utils.incite.mergers.conftest",
    # -- Managers
    "test_utils.managers.conftest",
    "test_utils.managers.contest.conftest",
    "test_utils.managers.gr.conftest",
    "test_utils.managers.ledger.conftest",
    "test_utils.managers.thl.conftest",
    "test_utils.managers.upk.conftest",
    # -- Models
    "test_utils.models.conftest",
    "test_utils.models.contest.conftest",
    "test_utils.models.gr.conftest",
    "test_utils.models.ledger.conftest",
    "test_utils.models.thl.conftest",
    "test_utils.models.upk.conftest",
    # -- Marketplaces
    "test_utils.precision.conftest",
    "test_utils.spectrum.conftest",
]
