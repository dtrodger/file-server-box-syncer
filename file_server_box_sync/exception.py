"""
Exceptions
"""


class SentinelException(Exception):
    pass


class SentinelTypeError(TypeError):
    pass


class SentinelValueError(ValueError):
    pass


class SentinelInvalidHTTPMethodError(SentinelValueError):
    pass


class SentinelFileError(SentinelException):
    pass
