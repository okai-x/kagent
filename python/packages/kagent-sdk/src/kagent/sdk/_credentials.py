"""Read projected credentials at the RPC boundary, including after token rotation."""

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path

import grpc.aio


def _read_token(path: Path) -> str:
    try:
        token = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as error:
        raise ValueError("platform token file is unavailable") from error
    if not token or any(character.isspace() for character in token):
        raise ValueError("platform token file must contain one bearer token")
    return token


class _Token:
    def __init__(self, path: Path) -> None:
        self.path = path

    async def forward(
        self,
        continuation: Callable[..., Awaitable[object]],
        details: grpc.aio.ClientCallDetails,
        request: object,
    ) -> object:
        token = await asyncio.to_thread(_read_token, self.path)
        metadata = [(key, value) for key, value in details.metadata or () if key.lower() != "authorization"]
        metadata.append(("authorization", f"Bearer {token}"))
        authenticated = grpc.aio.ClientCallDetails(
            details.method, details.timeout, metadata, details.credentials, details.wait_for_ready
        )
        return await continuation(authenticated, request)


class _UnaryUnary(_Token, grpc.aio.UnaryUnaryClientInterceptor):
    async def intercept_unary_unary(self, continuation, client_call_details, request):
        return await self.forward(continuation, client_call_details, request)


class _UnaryStream(_Token, grpc.aio.UnaryStreamClientInterceptor):
    async def intercept_unary_stream(self, continuation, client_call_details, request):
        return await self.forward(continuation, client_call_details, request)


class _StreamUnary(_Token, grpc.aio.StreamUnaryClientInterceptor):
    async def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        return await self.forward(continuation, client_call_details, request_iterator)


class _StreamStream(_Token, grpc.aio.StreamStreamClientInterceptor):
    async def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        return await self.forward(continuation, client_call_details, request_iterator)


def token_interceptors(path: str | Path | None) -> tuple[grpc.aio.ClientInterceptor, ...]:
    if path is None:
        return ()
    source = Path(path)
    return (_UnaryUnary(source), _UnaryStream(source), _StreamUnary(source), _StreamStream(source))
