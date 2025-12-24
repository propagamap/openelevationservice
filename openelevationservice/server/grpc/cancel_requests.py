import grpc
import threading
from typing import Optional

_memory = threading.local()


class RequestCancelledException(Exception):
    """Custom exception for cancelled requests."""

    pass


def grpc_start(context: grpc.ServicerContext) -> None:
    """Set the gRPC context in thread-local storage."""
    setattr(_memory, "context", context)


def grpc_check() -> None:
    """
    Check if the gRPC context is active, raise an exception if cancelled.

    Call this function before any long operation.
    """
    context: Optional[grpc.ServicerContext] = getattr(_memory, "context", None)
    if context is None or not context.is_active():
        raise RequestCancelledException()


def grpc_end() -> None:
    """Clear the gRPC context from thread-local storage."""
    if hasattr(_memory, "context"):
        delattr(_memory, "context")
