"""gRPC unary methods: the status codes a call ends with, and the exception that picks one.

Register a method with ``app.unary_unary("/package.Service/Method")``; its handler takes the
request message as ``bytes`` and returns the reply message as ``bytes``, encoded and decoded
by the app (with ``protobuf``, for example)::

    from mipyre.grpc import GrpcError, StatusCode
    import benchmark_pb2

    @app.unary_unary("/benchmark.BenchmarkService/GetSum")
    def get_sum(message: bytes) -> bytes:
        request = benchmark_pb2.SumRequest.FromString(message)
        if request.a < 0:
            raise GrpcError(StatusCode.INVALID_ARGUMENT, "a is negative")
        return benchmark_pb2.SumReply(result=request.a + request.b).SerializeToString()
"""

from enum import IntEnum

from mipyre.engine import GrpcError


class StatusCode(IntEnum):
    """The gRPC status codes, by grpcio's ``grpc.StatusCode`` names and the wire's numbers."""

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16


__all__ = ["GrpcError", "StatusCode"]
