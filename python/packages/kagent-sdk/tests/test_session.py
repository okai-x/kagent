import asyncio
from contextlib import aclosing

import grpc
import pytest
import pytest_asyncio
from a2a.types import a2a_pb2 as a2a
from a2a.types import a2a_pb2_grpc
from google.protobuf import struct_pb2
from kagent.api.v1 import checkpoints_pb2, checkpoints_pb2_grpc, sessions_pb2, sessions_pb2_grpc

from kagent.sdk import KagentClient


class Sessions(sessions_pb2_grpc.SessionServiceServicer):
    def __init__(self):
        self.requests = []
        self.metadata = []
        self.session = sessions_pb2.Session(id="session-1", agent={"namespace": "products", "name": "blender"})
        self.fail = False

    async def CreateSession(self, request, context):
        self.requests.append(request)
        self.metadata.append(dict(context.invocation_metadata()))
        return sessions_pb2.CreateSessionResponse(session=self.session)

    async def GetSession(self, request, context):
        self.requests.append(request)
        self.metadata.append(dict(context.invocation_metadata()))
        if self.fail:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "session access denied")
        return sessions_pb2.GetSessionResponse(session=self.session)


class Tasks(a2a_pb2_grpc.A2AServiceServicer):
    def __init__(self):
        self.requests = []
        self.metadata = []
        self.deadlines = []
        self.detached = asyncio.Event()
        self.cancel_calls = 0
        self.fail_send = False
        self.task = a2a.Task(id="task-1", context_id="session-1", status={"state": a2a.TASK_STATE_WORKING})

    def record(self, request, context):
        self.requests.append(request)
        self.metadata.append(dict(context.invocation_metadata()))
        self.deadlines.append(context.time_remaining())

    async def SendStreamingMessage(self, request, context):
        self.record(request, context)
        if self.fail_send:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "response lost")
        try:
            yield a2a.StreamResponse(task=self.task)
            await asyncio.Event().wait()
        finally:
            self.detached.set()

    async def SubscribeToTask(self, request, context):
        self.record(request, context)
        try:
            yield a2a.StreamResponse(task=self.task)
            await asyncio.Event().wait()
        finally:
            self.detached.set()

    async def GetTask(self, request, context):
        self.record(request, context)
        return self.task

    async def ListTasks(self, request, context):
        self.record(request, context)
        return a2a.ListTasksResponse(tasks=[self.task], next_page_token="next-page", page_size=1, total_size=2)

    async def CancelTask(self, request, context):
        self.record(request, context)
        self.cancel_calls += 1
        self.task.status.state = a2a.TASK_STATE_CANCELED
        return self.task


class Checkpoints(checkpoints_pb2_grpc.CheckpointServiceServicer):
    async def GetCheckpoint(self, request, context):
        self.metadata = dict(context.invocation_metadata())
        return checkpoints_pb2.GetCheckpointResponse(
            checkpoint={"id": request.checkpoint_id, "session_id": "session-1"}
        )


@pytest_asyncio.fixture
async def platform():
    server = grpc.aio.server()
    sessions, tasks, checkpoints = Sessions(), Tasks(), Checkpoints()
    sessions_pb2_grpc.add_SessionServiceServicer_to_server(sessions, server)
    a2a_pb2_grpc.add_A2AServiceServicer_to_server(tasks, server)
    checkpoints_pb2_grpc.add_CheckpointServiceServicer_to_server(checkpoints, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with KagentClient(f"http://127.0.0.1:{port}", user_id="alice", token="test-token", timeout=5) as client:
            yield client, sessions, tasks, checkpoints
    finally:
        await server.stop(None)


@pytest.mark.asyncio
async def test_create_and_get_session_keep_identity_and_request_id(platform):
    client, sessions, _, _ = platform
    session = await client.create_session("blender", request_id="saved-request", namespace="products", name="3D work")
    assert session == await client.get_session(session.id)
    assert sessions.requests == [
        sessions_pb2.CreateSessionRequest(
            agent={"namespace": "products", "name": "blender"}, request_id="saved-request", name="3D work"
        ),
        sessions_pb2.GetSessionRequest(session_id="session-1"),
    ]
    for metadata in sessions.metadata:
        assert metadata["x-user-id"] == "alice"
        assert metadata["authorization"] == "Bearer test-token"


@pytest.mark.asyncio
async def test_multiple_parts_preserve_content_order_and_task_continuation(platform):
    client, sessions, tasks, _ = platform
    parts = [
        a2a.Part(text="Make this part red.\nKeep its shape."),
        a2a.Part(
            data=struct_pb2.Value(
                struct_value=struct_pb2.Struct(fields={"partId": struct_pb2.Value(string_value="roof")})
            )
        ),
        a2a.Part(raw=b"image-bytes", filename="selection.png", media_type="image/png"),
    ]
    async with aclosing(
        client.stream_parts("session-1", parts, message_id="saved-message", task_id="task-1")
    ) as stream:
        assert (await anext(stream)).task.status.state == a2a.TASK_STATE_WORKING
    await asyncio.wait_for(tasks.detached.wait(), timeout=1)
    assert tasks.requests == [
        a2a.SendMessageRequest(
            tenant="products/blender",
            message=a2a.Message(
                message_id="saved-message", context_id="session-1", task_id="task-1", role=a2a.ROLE_USER, parts=parts
            ),
        )
    ]
    assert tasks.cancel_calls == 0
    assert tasks.task.status.state == a2a.TASK_STATE_WORKING
    for metadata in sessions.metadata + tasks.metadata:
        assert metadata["x-user-id"] == "alice"
        assert metadata["authorization"] == "Bearer test-token"
        assert "kagent-sandbox-id" not in metadata
    assert all(0 < deadline <= 5.5 for deadline in tasks.deadlines)


@pytest.mark.asyncio
@pytest.mark.parametrize("observer", ["stream_message", "subscribe_task"])
async def test_closing_observer_does_not_cancel_task(platform, observer):
    client, _, tasks, _ = platform
    stream = (
        client.stream_message("session-1", "hello", message_id="saved-message")
        if observer == "stream_message"
        else client.subscribe_task("session-1", "task-1")
    )
    async with aclosing(stream):
        assert (await anext(stream)).task.id == "task-1"
    await asyncio.wait_for(tasks.detached.wait(), timeout=1)
    assert tasks.cancel_calls == 0
    assert (await client.get_task("session-1", "task-1")).status.state == a2a.TASK_STATE_WORKING
    assert (await client.cancel_task("session-1", "task-1")).status.state == a2a.TASK_STATE_CANCELED
    assert tasks.cancel_calls == 1
    assert tasks.requests[-1] == a2a.CancelTaskRequest(tenant="products/blender", id="task-1")
    for metadata in tasks.metadata:
        assert metadata["x-user-id"] == "alice"
        assert metadata["authorization"] == "Bearer test-token"


@pytest.mark.asyncio
@pytest.mark.parametrize("history_length", [None, 0, 3])
async def test_task_queries_preserve_optional_history_and_pagination(platform, history_length):
    client, sessions, tasks, _ = platform
    assert (await client.get_task("session-1", "task-1", history_length=history_length)).id == "task-1"
    page = await client.list_tasks("session-1", page_size=1, page_token="page-1", history_length=history_length)
    assert [task.id for task in page.tasks] == ["task-1"]
    assert page.next_page_token == "next-page"
    assert page.total_size == 2
    assert tasks.requests == [
        a2a.GetTaskRequest(tenant="products/blender", id="task-1", history_length=history_length),
        a2a.ListTasksRequest(
            tenant="products/blender",
            context_id="session-1",
            page_size=1,
            page_token="page-1",
            history_length=history_length,
        ),
    ]
    assert all(request.HasField("history_length") == (history_length is not None) for request in tasks.requests)
    for metadata in sessions.metadata + tasks.metadata:
        assert metadata["x-user-id"] == "alice"
        assert metadata["authorization"] == "Bearer test-token"


@pytest.mark.asyncio
async def test_send_failure_is_exposed_without_automatic_retry(platform):
    client, _, tasks, _ = platform
    tasks.fail_send = True
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await anext(client.stream_message("session-1", "hello", message_id="saved-message"))
    assert error.value.code() == grpc.StatusCode.UNAVAILABLE
    assert len(tasks.requests) == 1
    assert tasks.cancel_calls == 0


@pytest.mark.asyncio
async def test_session_failure_prevents_a2a_dispatch(platform):
    client, sessions, tasks, _ = platform
    sessions.fail = True
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await anext(client.stream_message("session-1", "hello", message_id="saved-message"))
    assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert len(sessions.requests) == 1
    assert tasks.requests == []


@pytest.mark.asyncio
async def test_empty_parts_fail_before_network_dispatch(platform):
    client, sessions, tasks, _ = platform
    with pytest.raises(ValueError, match="parts must not be empty"):
        await anext(client.stream_parts("session-1", [], message_id="saved-message"))
    assert sessions.requests == tasks.requests == []


@pytest.mark.asyncio
async def test_public_checkpoint_stub_forwards_identity(platform):
    client, _, _, checkpoints = platform
    response = await client.checkpoints.GetCheckpoint(
        checkpoints_pb2.GetCheckpointRequest(checkpoint_id="checkpoint-1"), **client.call_options()
    )
    assert response.checkpoint.id == "checkpoint-1"
    assert response.checkpoint.session_id == "session-1"
    assert checkpoints.metadata["x-user-id"] == "alice"
    assert checkpoints.metadata["authorization"] == "Bearer test-token"
