class BuildItemsError(Exception):

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class BuildError(Exception):

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class FetchError(Exception):

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
