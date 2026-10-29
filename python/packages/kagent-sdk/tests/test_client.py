from pathlib import Path

import grpc
import pytest
import pytest_asyncio
from ateenv.v1alpha import guest_pb2 as guest
from ateenv.v1alpha import guest_pb2_grpc

from kagent.sdk import KagentClient


class Files(guest_pb2_grpc.FileSystemServiceServicer):
    def __init__(self):
        self.data = b""
        self.metadata = {}
        self.path = None

    async def WriteFile(self, request_iterator, context):
        self.metadata = dict(context.invocation_metadata())
        self.data = b""
        async for request in request_iterator:
            if self.path is None:
                self.path = request.path
            self.data += request.chunk
        return guest.WriteFileResponse(bytes_written=len(self.data))

    async def ReadFile(self, request, context):
        yield guest.ReadFileResponse(chunk=b"partial")
        if request.path == "broken":
            await context.abort(grpc.StatusCode.NOT_FOUND, "file vanished")
        yield guest.ReadFileResponse(chunk=b"complete")


@pytest_asyncio.fixture
async def server():
    server = grpc.aio.server()
    service = Files()
    guest_pb2_grpc.add_FileSystemServiceServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with KagentClient(f"http://127.0.0.1:{port}", user_id="alice", token="test-token") as client:
            yield client, service
    finally:
        await server.stop(None)


@pytest.mark.asyncio
@pytest.mark.parametrize("data", [b"", bytes(range(256)) * 3000])
async def test_upload_chunks_and_routing(server, tmp_path, data):
    client, service = server
    source = tmp_path / "source"
    source.write_bytes(data)
    assert await client.upload("sandbox-id", source, "/data/workspace/file") == len(data)
    assert service.data == data
    assert service.path == "/data/workspace/file"
    assert service.metadata["kagent-sandbox-id"] == "sandbox-id"
    assert service.metadata["x-user-id"] == "alice"
    assert service.metadata["authorization"] == "Bearer test-token"


@pytest.mark.asyncio
async def test_failed_download_preserves_destination(server, tmp_path):
    client, _ = server
    target = tmp_path / "artifact"
    target.write_bytes(b"original")
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await client.download("sandbox-id", "broken", target)
    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    assert target.read_bytes() == b"original"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.asyncio
async def test_successful_download_replaces_destination(server, tmp_path):
    client, _ = server
    target = tmp_path / "artifact"
    target.write_bytes(b"original")
    assert await client.download("sandbox-id", "ok", target) == len(b"partialcomplete")
    assert target.read_bytes() == b"partialcomplete"
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("url", ["ftp://host", "http://user:secret@host", "http://host/path", "http://host?token=x"])
def test_invalid_origin(url):
    with pytest.raises(ValueError):
        KagentClient(url)
