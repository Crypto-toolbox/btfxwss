class BitfinexInfo(Warning):
    def __init__(self, code: int, description: str):
        self.code = code
        self.description = description
        message = "Code {}: {}".format(code, description)
        super(BitfinexInfo, self).__init__(message)


class BitfinexError(Exception):
    def __init__(self, code: int, description: str):
        self.code = code
        self.description = description
        message = "Code {}: {}".format(code, description)
        super(BitfinexError, self).__init__(message)


class NoPongReceived(ConnectionError):
    pass


class UnhandledEvent(NotImplementedError):
    def __init__(self, unhandled_event):
        self.event = unhandled_event
        message = "Received an event which we could not process: {}".format(unhandled_event)
        super(UnhandledEvent, self).__init__(message)
