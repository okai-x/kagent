"""History pages preserve native bytes and durable cutoffs without hidden retries."""

import grpc
import pytest
import pytest_asyncio
from kagent.api.v1 import sessions_pb2, sessions_pb2_grpc

from kagent.sdk import KagentClient


class History(sessions_pb2_grpc.SessionServiceServicer):
    def __init__(self):
        self.requests = []
        self.metadata = []
        self.error = None
        self.cutoff = sessions_pb2.SessionHistoryCutoff(history_id="history", through_sequence=19)
        self.raw = b"\x82\xa6\x0e\x06future" * (1 << 19)

    async def ExportSessionHistory(self, request, context):
        self.requests.append(request)
        self.metadata.append(dict(context.invocation_metadata()))
        if self.error:
            await context.abort(self.error, "history export failed")
        return sessions_pb2.ExportSessionHistoryResponse(
            cutoff=self.cutoff,
            events=[sessions_pb2.SessionHistoryEvent(sequence=19, task_id="task", data=self.raw)],
            next_after_sequence=19,
            has_more=False,
        )


@pytest_asyncio.fixture
async def platform():
    service, server = History(), grpc.aio.server()
    sessions_pb2_grpc.add_SessionServiceServicer_to_server(service, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with KagentClient(f"http://127.0.0.1:{port}", token="test-credential", timeout=5) as client:
            yield client, service
    finally:
        await server.stop(None)


@pytest.mark.asyncio
async def test_history_preserves_large_raw_event_cutoff_and_identity(platform):
    client, service = platform
    first = await client.export_session_history("session", page_size=1)
    assert not service.requests[0].HasField("cutoff")
    assert first.events[0].data == service.raw
    assert first.cutoff == service.cutoff
    assert not first.has_more
    await client.export_session_history(
        "session", cutoff=first.cutoff, after_sequence=first.next_after_sequence, page_size=7
    )
    assert service.requests[1] == sessions_pb2.ExportSessionHistoryRequest(
        session_id="session", cutoff=first.cutoff, after_sequence=19, page_size=7
    )
    assert service.metadata[0]["authorization"] == "Bearer test-credential"
    assert "kagent-sandbox-id" not in service.metadata[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.RESOURCE_EXHAUSTED],
)
async def test_history_preserves_failure_without_retry(platform, code):
    client, service = platform
    service.error = code
    with pytest.raises(grpc.aio.AioRpcError) as failure:
        await client.export_session_history("session")
    assert failure.value.code() == code
    assert len(service.requests) == 1
