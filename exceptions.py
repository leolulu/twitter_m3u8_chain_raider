class NoFetishError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class RestartBrowserWarning(Warning):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
