"""The SDK preserves the workspace read fence and never treats partial bytes as success."""

import asyncio
from contextlib import aclosing

import grpc
import pytest
import pytest_asyncio
from google.rpc import error_details_pb2, status_pb2
from grpc_status import rpc_status
from kagent.api.v1 import sessions_pb2, sessions_pb2_grpc

from kagent.sdk import KagentClient


class Workspace(sessions_pb2_grpc.SessionServiceServicer):
    def __init__(self):
        self.requests = []
        self.metadata = []
        self.error = None
        self.error_metadata = ()
        self.wait = False
        self.closed = asyncio.Event()

    async def ReadWorkspaceFile(self, request, context):
        self.requests.append(request)
        self.metadata.append(dict(context.invocation_metadata()))
        try:
            yield sessions_pb2.ReadWorkspaceFileResponse(chunk=b"first")
            if self.error:
                await context.abort(self.error, "workspace read did not complete", self.error_metadata)
            if self.wait:
                await asyncio.Event().wait()
            yield sessions_pb2.ReadWorkspaceFileResponse(chunk=b"second")
        finally:
            self.closed.set()


@pytest_asyncio.fixture
async def platform():
    service, server = Workspace(), grpc.aio.server()
    sessions_pb2_grpc.add_SessionServiceServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with KagentClient(f"http://127.0.0.1:{port}", token="test-credential", timeout=5) as client:
            yield client, service
    finally:
        await server.stop(None)


@pytest.mark.asyncio
async def test_workspace_stream_preserves_head_path_and_identity(platform):
    client, service = platform
    chunks = [chunk async for chunk in client.read_workspace_file("session", "result.glb", "terminal-task")]
    assert chunks == [b"first", b"second"]
    assert service.requests == [
        sessions_pb2.ReadWorkspaceFileRequest(
            session_id="session", path="result.glb", expected_head_task_id="terminal-task"
        )
    ]
    assert service.metadata[0]["authorization"] == "Bearer test-credential"
    assert "kagent-sandbox-id" not in service.metadata[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code", [grpc.StatusCode.NOT_FOUND, grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.UNAVAILABLE]
)
async def test_workspace_partial_failure_preserves_status_without_retry(platform, code):
    client, service = platform
    service.error = code
    received = []
    with pytest.raises(grpc.aio.AioRpcError) as failure:
        async for chunk in client.read_workspace_file("session", "result.glb", "terminal-task"):
            received.append(chunk)
    assert received == [b"first"]
    assert failure.value.code() == code
    assert len(service.requests) == 1


@pytest.mark.asyncio
async def test_closing_workspace_iterator_cancels_the_read(platform):
    client, service = platform
    service.wait = True
    async with aclosing(client.read_workspace_file("session", "result.glb", "terminal-task")) as stream:
        assert await anext(stream) == b"first"
    await asyncio.wait_for(service.closed.wait(), 1)
    assert len(service.requests) == 1


@pytest.mark.asyncio
async def test_workspace_structured_file_missing_survives_transport(platform):
    client, service = platform
    service.error = grpc.StatusCode.NOT_FOUND
    status = status_pb2.Status(code=5, message="workspace read did not complete")
    expected = error_details_pb2.ErrorInfo(reason="WORKSPACE_FILE_NOT_FOUND", domain="kagent.dev")
    status.details.add().Pack(expected)
    service.error_metadata = (("grpc-status-details-bin", status.SerializeToString()),)
    with pytest.raises(grpc.aio.AioRpcError) as failure:
        async for _chunk in client.read_workspace_file("session", "result.glb", "terminal-task"):
            pass
    actual = rpc_status.from_call(failure.value)
    assert actual is not None
    info = error_details_pb2.ErrorInfo()
    assert actual.details[0].Unpack(info)
    assert info == expected
    assert len(service.requests) == 1
