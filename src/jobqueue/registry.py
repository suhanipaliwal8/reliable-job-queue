from typing import Callable

_HANDLERS: dict[str, Callable[[dict], None]] = {}


def handler(job_type: str):
    """Decorator: register a function as the handler for a job type.

        @handler("send_email")
        def send_email(job): ...
    """
    def decorator(func):
        _HANDLERS[job_type] = func
        return func
    return decorator


def get_handler(job_type: str):
    return _HANDLERS.get(job_type)