from pathlib import Path

import grpc
import pytest

from kagent.sdk import KagentClient


@pytest.mark.asyncio
async def test_projected_token_rotation_reaches_every_rpc_shape(tmp_path: Path):
    observed = []

    async def unary(request, context):
        observed.append(dict(context.invocation_metadata())["authorization"])
        return request

    async def unary_stream(request, context):
        yield await unary(request, context)

    async def stream_unary(requests, context):
        return await unary(b"".join([request async for request in requests]), context)

    async def stream_stream(requests, context):
        async for request in requests:
            yield await unary(request, context)

    server = grpc.aio.server()
    server.add_generic_rpc_handlers(
        [
            grpc.method_handlers_generic_handler(
                "credentials",
                {
                    "Unary": grpc.unary_unary_rpc_method_handler(unary),
                    "ServerStream": grpc.unary_stream_rpc_method_handler(unary_stream),
                    "ClientStream": grpc.stream_unary_rpc_method_handler(stream_unary),
                    "Bidi": grpc.stream_stream_rpc_method_handler(stream_stream),
                },
            )
        ]
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    token = tmp_path / "projected-token"
    try:
        async with KagentClient(f"http://127.0.0.1:{port}", token_file=token) as client:
            for value in ("first-token", "rotated-token"):
                replacement = tmp_path / "replacement"
                replacement.write_text(value)
                replacement.replace(token)
                assert await client.channel.unary_unary("/credentials/Unary")(b"body") == b"body"
                assert [data async for data in client.channel.unary_stream("/credentials/ServerStream")(b"body")] == [
                    b"body"
                ]
                assert await client.channel.stream_unary("/credentials/ClientStream")(iter([b"body"])) == b"body"
                assert [data async for data in client.channel.stream_stream("/credentials/Bidi")(iter([b"body"]))] == [
                    b"body"
                ]
            assert observed == ["Bearer first-token"] * 4 + ["Bearer rotated-token"] * 4
            token.unlink()
            with pytest.raises(ValueError, match="unavailable"):
                await client.channel.unary_unary("/credentials/Unary")(b"body")
            token.write_text("\n")
            with pytest.raises(ValueError, match="one bearer token"):
                await client.channel.unary_unary("/credentials/Unary")(b"body")
            assert len(observed) == 8
    finally:
        await server.stop(None)


def test_static_and_projected_token_cannot_be_combined(tmp_path: Path):
    with pytest.raises(ValueError, match="either token or token_file"):
        KagentClient(token="secret", token_file=tmp_path / "token")
