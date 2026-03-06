class LedgerAccountDoesntExistError(Exception):
    pass


class LedgerTransactionDoesntExistError(Exception):
    pass


class LedgerTransactionCreateError(Exception):
    """
    Ledger transaction creation failed
    """

    pass


class LedgerTransactionCreateLockError(LedgerTransactionCreateError):
    """
    Ledger transaction creation failed because we could not acquire a lock
    """

    pass


class LedgerTransactionReleaseLockError(LedgerTransactionCreateError):
    """
    There was an error releasing the redis lock. I'm not exactly sure why this
        happens sometimes, but it does. Seems to be almost always during
        back-populate as in sentry I see this very rarely.
    """

    pass


class LedgerTransactionFlagAlreadyExistsError(LedgerTransactionCreateError):
    """
    Ledger transaction creation failed because the redis flag for this
    tx was already set
    """

    pass


class LedgerTransactionConditionFailedError(LedgerTransactionCreateError):
    """
    We tried to create a transaction but the condition check failed.
    """

    pass
