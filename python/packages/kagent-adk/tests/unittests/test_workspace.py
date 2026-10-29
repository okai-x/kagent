import asyncio
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from kagent.core import AsyncControllerClient
from kagent.runtime.v1 import workspace_pb2, workspace_pb2_grpc

from kagent.adk._workspace import WorkspaceService, _file_operation
from kagent.adk._workspace_file import CHUNK_BYTES, MAX_FILE_BYTES, WorkspaceFile


class AccessService(workspace_pb2_grpc.WorkspaceAccessServiceServicer):
    def __init__(self):
        self.calls = []
        self.expires_at = time.time() + 60
        self.denial = None

    async def AuthorizeRead(self, request, context):
        self.calls.append((request, dict(context.invocation_metadata())))
        if self.denial:
            await context.abort(self.denial, "private controller detail")
        if request.grant_id != "test-grant" or request.path != "result.glb":
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "wrong grant binding")
        expiry = Timestamp()
        expiry.FromNanoseconds(int(self.expires_at * 1e9))
        return workspace_pb2.AuthorizeReadResponse(expires_at=expiry)


@dataclass
class Runtime:
    root: Path
    access: AccessService
    stub: workspace_pb2_grpc.WorkspaceServiceStub

    def read(self, path="result.glb", grant_id="test-grant", **kwargs):
        return self.stub.ReadFile(workspace_pb2.ReadFileRequest(grant_id=grant_id, path=path), **kwargs)


@pytest.fixture
async def runtime(tmp_path):
    access = AccessService()
    control = grpc.aio.server()
    workspace_pb2_grpc.add_WorkspaceAccessServiceServicer_to_server(access, control)
    control_port = control.add_insecure_port("127.0.0.1:0")
    await control.start()
    controller = AsyncControllerClient(f"http://127.0.0.1:{control_port}", agent_name="test-agent")
    server = grpc.aio.server()
    workspace_pb2_grpc.add_WorkspaceServiceServicer_to_server(WorkspaceService(controller, tmp_path), server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            yield Runtime(tmp_path, access, workspace_pb2_grpc.WorkspaceServiceStub(channel))
    finally:
        await server.stop(grace=0)
        await controller.close()
        await control.stop(grace=0)


async def assert_failure(call, code):
    with pytest.raises(grpc.aio.AioRpcError) as error:
        while await call.read() is not grpc.aio.EOF:
            pass
    assert error.value.code() is code
    assert "private controller detail" not in error.value.details()


async def test_grpc_read_stream_uses_own_controller_identity(runtime):
    contents = b"glTF" + b"x" * (CHUNK_BYTES * 2)
    (runtime.root / "result.glb").write_bytes(contents)
    chunks = [part.chunk async for part in runtime.read(metadata=(("authorization", "Bearer forged"),))]
    assert b"".join(chunks) == contents
    assert all(0 < len(chunk) <= CHUNK_BYTES for chunk in chunks)
    ((request, metadata),) = runtime.access.calls
    assert request.grant_id == "test-grant"
    assert request.path == "result.glb"
    assert metadata["authorization"] == "Bearer placeholder"
    assert metadata["x-agent-name"] == "test-agent"


@pytest.mark.parametrize("path", ["", "/etc/passwd", "../outside", "a/../result.glb", "a//b", "./a", "a/", "a\x00b"])
async def test_invalid_paths_never_contact_authorizer(runtime, path):
    await assert_failure(runtime.read(path=path), grpc.StatusCode.INVALID_ARGUMENT)
    assert runtime.access.calls == []


@pytest.mark.parametrize("grant_id", ["", "forged-grant"])
async def test_wrong_grant_never_opens_file(runtime, monkeypatch, grant_id):
    def unexpected_open(_):
        pytest.fail("an unauthorized request opened the workspace")

    monkeypatch.setattr(WorkspaceFile, "open", unexpected_open)
    await assert_failure(runtime.read(grant_id=grant_id), grpc.StatusCode.PERMISSION_DENIED)


async def test_path_bound_grant_is_not_reusable(runtime):
    (runtime.root / "secret.txt").write_text("not downloadable")
    await assert_failure(runtime.read(path="secret.txt"), grpc.StatusCode.PERMISSION_DENIED)


@pytest.mark.parametrize("denial", [grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.UNAVAILABLE])
async def test_authorization_error_does_not_read_or_expose_details(runtime, monkeypatch, denial):
    runtime.access.denial = denial
    monkeypatch.setattr(WorkspaceFile, "open", lambda _: pytest.fail("read attempted after authorization failure"))
    await assert_failure(runtime.read(), denial)


async def test_expired_grant_never_opens_file(runtime, monkeypatch):
    runtime.access.expires_at = time.time() - 1
    monkeypatch.setattr(WorkspaceFile, "open", lambda _: pytest.fail("expired grant opened a file"))
    await assert_failure(runtime.read(), grpc.StatusCode.PERMISSION_DENIED)


async def test_missing_file_is_not_found(runtime):
    await assert_failure(runtime.read(), grpc.StatusCode.NOT_FOUND)


@pytest.mark.parametrize("kind", ["symlink", "directory", "fifo", "device"])
async def test_non_regular_files_are_rejected_without_blocking(runtime, monkeypatch, kind):
    path = runtime.root / "result.glb"
    if kind == "symlink":
        path.symlink_to("/etc/passwd")
    elif kind == "directory":
        path.mkdir()
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        original_open = os.open

        def open_device(name, flags, **kwargs):
            if name == "result.glb":
                return original_open("/dev/null", flags)
            return original_open(name, flags, **kwargs)

        monkeypatch.setattr(os, "open", open_device)
    await assert_failure(runtime.read(timeout=5), grpc.StatusCode.FAILED_PRECONDITION)


async def test_oversized_file_returns_error_instead_of_successful_eof(runtime):
    with (runtime.root / "result.glb").open("wb") as stream:
        stream.truncate(MAX_FILE_BYTES + 1)
    await assert_failure(runtime.read(), grpc.StatusCode.RESOURCE_EXHAUSTED)


async def test_exact_limit_and_empty_file_succeed(runtime):
    path = runtime.root / "result.glb"
    path.touch()
    assert [chunk async for chunk in runtime.read()] == []
    with path.open("wb") as stream:
        stream.truncate(MAX_FILE_BYTES)
    size = sum([len(chunk.chunk) async for chunk in runtime.read()])
    assert size == MAX_FILE_BYTES


async def test_expiration_during_read_fails_stream(runtime, monkeypatch):
    (runtime.root / "result.glb").write_bytes(b"x" * CHUNK_BYTES * 2)
    original_read = WorkspaceFile.read
    original_expiry = WorkspaceService._check_expiry
    reads = 0

    def read(reader):
        nonlocal reads
        reads += 1
        return original_read(reader)

    async def check_expiry(expires_at, context):
        await original_expiry(0 if reads >= 2 else expires_at, context)

    monkeypatch.setattr(WorkspaceFile, "read", read)
    monkeypatch.setattr(WorkspaceService, "_check_expiry", staticmethod(check_expiry))
    call = runtime.read()
    assert len((await call.read()).chunk) == CHUNK_BYTES
    await assert_failure(call, grpc.StatusCode.PERMISSION_DENIED)


async def test_cancelled_rpc_joins_io_and_closes_descriptor(runtime, monkeypatch):
    (runtime.root / "result.glb").write_bytes(b"x" * CHUNK_BYTES * 2)
    loop = asyncio.get_running_loop()
    reading = asyncio.Event()
    closed = asyncio.Event()
    release = threading.Event()
    original_read = WorkspaceFile.read
    original_close = WorkspaceFile.close
    observed = []

    def read(reader):
        observed.append(reader.fd)
        loop.call_soon_threadsafe(reading.set)
        assert release.wait(timeout=5)
        return original_read(reader)

    def close(reader):
        original_close(reader)
        closed.set()

    monkeypatch.setattr(WorkspaceFile, "read", read)
    monkeypatch.setattr(WorkspaceFile, "close", close)
    call = runtime.read()
    try:
        await asyncio.wait_for(reading.wait(), 5)
        assert call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call.read()
    finally:
        release.set()
    await asyncio.wait_for(closed.wait(), 5)
    with pytest.raises(OSError):
        os.fstat(observed[0])


@pytest.mark.parametrize("operation_name", ["open", "read"])
async def test_repeated_cancellation_joins_file_operation(tmp_path: Path, operation_name: str) -> None:
    (tmp_path / "result.glb").write_bytes(b"glTF")
    reader = WorkspaceFile(tmp_path, ["result.glb"])
    if operation_name == "read":
        reader.open()
    operation = reader.open if operation_name == "open" else reader.read
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    closed = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    observed_fd: int | None = None

    def blocked_operation() -> bytes | None:
        nonlocal observed_fd
        loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(timeout=5)
            result = operation()
            observed_fd = reader.fd
            return result
        finally:
            finished.set()

    async def download() -> None:
        try:
            await _file_operation(blocked_operation)
        finally:
            reader.close()
            closed.set()

    task = asyncio.create_task(download())
    try:
        await asyncio.wait_for(started.wait(), 5)
        assert task.cancel("first cancellation")
        await asyncio.sleep(0)
        assert task.cancel("second cancellation")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not closed.is_set(), "the descriptor must remain open until its worker finishes"
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
        with pytest.raises(asyncio.CancelledError) as error:
            await task
        reader.close()
    assert error.value.args == ("first cancellation",)
    assert closed.is_set()
    assert observed_fd is not None
    with pytest.raises(OSError):
        os.fstat(observed_fd)
