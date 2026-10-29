from datetime import UTC, datetime, timedelta, timezone

import grpc
import pytest
import pytest_asyncio
from kagent.api.v1 import sandboxes_pb2 as sandbox
from kagent.api.v1 import sandboxes_pb2_grpc

from kagent.sdk import KagentClient


class Sandboxes(sandboxes_pb2_grpc.SandboxServiceServicer):
    def __init__(self):
        self.requests = []

    async def CreateSandbox(self, request, context):
        self.requests.append(request)
        return sandbox.CreateSandboxResponse(sandbox=sandbox.Sandbox(id="sandbox", session_id=request.session_id))

    async def RenewSandbox(self, request, context):
        self.requests.append(request)
        return sandbox.RenewSandboxResponse(
            sandbox=sandbox.Sandbox(id=request.sandbox_id, expires_at=request.expires_at)
        )


@pytest_asyncio.fixture
async def server():
    server = grpc.aio.server()
    service = Sandboxes()
    sandboxes_pb2_grpc.add_SandboxServiceServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with KagentClient(f"http://127.0.0.1:{port}") as client:
            yield client, service
    finally:
        await server.stop(None)


@pytest.mark.asyncio
async def test_sandbox_binding_and_absolute_renewal(server):
    client, service = server
    instance = await client.create_sandbox("scratch", request_id="stable-create", session_id="session")
    assert instance.session_id == "session"
    assert service.requests[0].session_id == "session"
    assert service.requests[0].request_id == "stable-create"
    deadline = datetime(2026, 10, 30, 12, 0, tzinfo=timezone(timedelta(hours=8)))
    for _ in range(2):
        renewed = await client.renew_sandbox(instance.id, deadline)
        assert renewed.expires_at.ToDatetime(tzinfo=UTC) == deadline
    assert service.requests[1] == service.requests[2]


@pytest.mark.asyncio
async def test_naive_renewal_deadline_never_reaches_server(server):
    client, service = server
    with pytest.raises(ValueError, match="timezone-aware"):
        await client.renew_sandbox("sandbox", datetime(2026, 10, 30, 12, 0))
    assert service.requests == []
