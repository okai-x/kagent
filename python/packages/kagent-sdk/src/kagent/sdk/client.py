from __future__ import annotations

import asyncio
import os
import tempfile
from collections.abc import AsyncIterator, Sequence
from contextlib import aclosing
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

import grpc
from a2a.types import a2a_pb2 as a2a
from a2a.types import a2a_pb2_grpc
from ateenv.v1alpha import guest_pb2 as guest
from ateenv.v1alpha import guest_pb2_grpc
from kagent.api.v1 import (
    checkpoints_pb2_grpc,
    common_pb2,
    sandboxes_pb2,
    sandboxes_pb2_grpc,
    sessions_pb2,
    sessions_pb2_grpc,
)

from ._credentials import token_interceptors


class KagentClient:
    """Async platform client. Mutations are never automatically retried.

    Keep request/message IDs across retries. A deadline or canceled stream does
    not imply the server rolled back an operation or stopped a process.
    ``user_id`` is only an identity selector for development authentication;
    production deployments should provide a bearer token over HTTPS.
    """

    def __init__(
        self,
        url: str = "http://127.0.0.1:8083",
        *,
        user_id: str | None = None,
        token: str | None = None,
        token_file: str | Path | None = None,
        timeout: float = 300,
    ) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in ("", "/")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("url must be an http(s) origin without credentials, query, or path")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if token is not None and token_file is not None:
            raise ValueError("provide either token or token_file, not both")
        options = [("grpc.max_receive_message_length", 16 << 20), ("grpc.max_send_message_length", 16 << 20)]
        interceptors = token_interceptors(token_file)
        self.channel = (
            grpc.aio.secure_channel(
                parsed.netloc, grpc.ssl_channel_credentials(), options=options, interceptors=interceptors
            )
            if parsed.scheme == "https"
            else grpc.aio.insecure_channel(parsed.netloc, options=options, interceptors=interceptors)
        )
        self.timeout = timeout
        self._metadata = []
        if user_id:
            self._metadata.append(("x-user-id", user_id))
        if token:
            self._metadata.append(("authorization", f"Bearer {token}"))
        self.sessions = sessions_pb2_grpc.SessionServiceStub(self.channel)
        self.checkpoints = checkpoints_pb2_grpc.CheckpointServiceStub(self.channel)
        self.sandboxes = sandboxes_pb2_grpc.SandboxServiceStub(self.channel)
        self.processes = guest_pb2_grpc.ProcessServiceStub(self.channel)
        self.files = guest_pb2_grpc.FileSystemServiceStub(self.channel)
        self.a2a = a2a_pb2_grpc.A2AServiceStub(self.channel)

    def call_options(self, sandbox_id: str | None = None) -> dict:
        """Authentication/deadline options, also usable with the public stubs."""
        metadata = list(self._metadata)
        if sandbox_id is not None:
            if not sandbox_id:
                raise ValueError("sandbox_id must not be empty")
            metadata.append(("kagent-sandbox-id", sandbox_id))
        return {"metadata": metadata, "timeout": self.timeout}

    async def __aenter__(self) -> KagentClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self.channel.close()

    async def create_session(
        self, agent: str, *, request_id: str, namespace: str = "kagent", name: str = ""
    ) -> sessions_pb2.Session:
        response = await self.sessions.CreateSession(
            sessions_pb2.CreateSessionRequest(
                agent=common_pb2.ResourceReference(namespace=namespace, name=agent), request_id=request_id, name=name
            ),
            **self.call_options(),
        )
        return response.session

    async def get_session(self, session_id: str) -> sessions_pb2.Session:
        return (
            await self.sessions.GetSession(sessions_pb2.GetSessionRequest(session_id=session_id), **self.call_options())
        ).session

    async def export_session_history(
        self,
        session_id: str,
        *,
        cutoff: sessions_pb2.SessionHistoryCutoff | None = None,
        after_sequence: int = 0,
        page_size: int = 100,
    ) -> sessions_pb2.ExportSessionHistoryResponse:
        """Read one bounded page of exact native event bytes without retrying.

        Persist the first response's cutoff before processing its events. Reuse
        it with next_after_sequence while has_more is true, or with zero to
        restart the same prefix. Later appends are excluded. An oversized event
        fails explicitly; this export does not contain runtime workspace files.
        """
        return await self.sessions.ExportSessionHistory(
            sessions_pb2.ExportSessionHistoryRequest(
                session_id=session_id, cutoff=cutoff, after_sequence=after_sequence, page_size=page_size
            ),
            **self.call_options(),
        )

    async def read_workspace_file(self, session_id: str, path: str, expected_head_task_id: str) -> AsyncIterator[bytes]:
        """Read one file at a settled terminal head; only successful EOF is complete.

        Errors retain their gRPC status/details. Closing the iterator cancels the
        read; callers must discard partial bytes and must not infer cleanup from
        a canceled stream. The platform enforces the 64 MiB file limit.
        """
        call = self.sessions.ReadWorkspaceFile(
            sessions_pb2.ReadWorkspaceFileRequest(
                session_id=session_id, path=path, expected_head_task_id=expected_head_task_id
            ),
            **self.call_options(),
        )
        try:
            async for response in call:
                yield response.chunk
        finally:
            call.cancel()

    async def read_workspace_files(
        self, session_id: str, paths: Sequence[str], expected_head_task_id: str
    ) -> AsyncIterator[sessions_pb2.ReadWorkspaceFilesResponse]:
        """Collect a bounded batch with one final runtime snapshot, without retries.

        Responses retain request order and include explicit empty/missing files.
        Discard the entire batch on any stream error; only final OK is complete.
        """
        call = self.sessions.ReadWorkspaceFiles(
            sessions_pb2.ReadWorkspaceFilesRequest(
                session_id=session_id, paths=paths, expected_head_task_id=expected_head_task_id
            ),
            **self.call_options(),
        )
        try:
            async for response in call:
                yield response
        finally:
            call.cancel()

    async def stream_message(self, session_id: str, text: str, *, message_id: str) -> AsyncIterator[a2a.StreamResponse]:
        """Send a text-only turn; closing the iterator does not cancel the task."""
        async with aclosing(self.stream_parts(session_id, [a2a.Part(text=text)], message_id=message_id)) as stream:
            async for event in stream:
                yield event

    async def stream_parts(
        self, session_id: str, parts: Sequence[a2a.Part], *, message_id: str, task_id: str = ""
    ) -> AsyncIterator[a2a.StreamResponse]:
        """Send upstream A2A parts unchanged, without adding prompt content.

        Persist ``message_id`` before sending. The SDK never retries a send;
        after an uncertain result, reconcile the task before submitting again.
        ``task_id`` continues an existing task that is waiting for user input.
        """
        if not parts:
            raise ValueError("parts must not be empty")
        message = a2a.Message(
            message_id=message_id, context_id=session_id, task_id=task_id, role=a2a.ROLE_USER, parts=parts
        )
        call = self.a2a.SendStreamingMessage(
            a2a.SendMessageRequest(tenant=await self._tenant(session_id), message=message),
            **self.call_options(),
        )
        try:
            async for event in call:
                yield event
        finally:
            call.cancel()

    async def get_task(self, session_id: str, task_id: str, *, history_length: int | None = None) -> a2a.Task:
        return await self.a2a.GetTask(
            a2a.GetTaskRequest(tenant=await self._tenant(session_id), id=task_id, history_length=history_length),
            **self.call_options(),
        )

    async def list_tasks(
        self,
        session_id: str,
        *,
        page_size: int | None = None,
        page_token: str = "",
        history_length: int | None = None,
    ) -> a2a.ListTasksResponse:
        """Return one page of tasks in this session; retain its next_page_token."""
        return await self.a2a.ListTasks(
            a2a.ListTasksRequest(
                tenant=await self._tenant(session_id),
                context_id=session_id,
                page_size=page_size,
                page_token=page_token,
                history_length=history_length,
            ),
            **self.call_options(),
        )

    async def subscribe_task(self, session_id: str, task_id: str) -> AsyncIterator[a2a.StreamResponse]:
        """Observe a known task. Closing the iterator only detaches this observer."""
        call = self.a2a.SubscribeToTask(
            a2a.SubscribeToTaskRequest(tenant=await self._tenant(session_id), id=task_id), **self.call_options()
        )
        try:
            async for event in call:
                yield event
        finally:
            call.cancel()

    async def cancel_task(self, session_id: str, task_id: str) -> a2a.Task:
        """Explicitly request task cancellation; a deadline leaves the outcome unknown."""
        return await self.a2a.CancelTask(
            a2a.CancelTaskRequest(tenant=await self._tenant(session_id), id=task_id), **self.call_options()
        )

    async def _tenant(self, session_id: str) -> str:
        session = await self.get_session(session_id)
        return f"{session.agent.namespace}/{session.agent.name}"

    async def create_sandbox(
        self,
        template: str,
        *,
        request_id: str,
        namespace: str = "kagent",
        ttl_seconds: int = 3600,
        name: str = "",
        session_id: str = "",
    ) -> sandboxes_pb2.Sandbox:
        response = await self.sandboxes.CreateSandbox(
            sandboxes_pb2.CreateSandboxRequest(
                sandbox_template=common_pb2.ResourceReference(namespace=namespace, name=template),
                request_id=request_id,
                name=name,
                ttl={"seconds": ttl_seconds},
                session_id=session_id,
            ),
            **self.call_options(),
        )
        return response.sandbox

    async def get_sandbox(self, sandbox_id: str) -> sandboxes_pb2.Sandbox:
        return (
            await self.sandboxes.GetSandbox(
                sandboxes_pb2.GetSandboxRequest(sandbox_id=sandbox_id), **self.call_options()
            )
        ).sandbox

    async def renew_sandbox(self, sandbox_id: str, expires_at: datetime) -> sandboxes_pb2.Sandbox:
        """Extend a product-owned lease to an absolute, timezone-aware deadline."""
        if expires_at.tzinfo is None or expires_at.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")
        request = sandboxes_pb2.RenewSandboxRequest(sandbox_id=sandbox_id)
        request.expires_at.FromDatetime(expires_at)
        return (await self.sandboxes.RenewSandbox(request, **self.call_options())).sandbox

    async def suspend_sandbox(self, sandbox_id: str) -> sandboxes_pb2.Sandbox:
        return (
            await self.sandboxes.SuspendSandbox(
                sandboxes_pb2.SuspendSandboxRequest(sandbox_id=sandbox_id), **self.call_options()
            )
        ).sandbox

    async def resume_sandbox(self, sandbox_id: str) -> sandboxes_pb2.Sandbox:
        return (
            await self.sandboxes.ResumeSandbox(
                sandboxes_pb2.ResumeSandboxRequest(sandbox_id=sandbox_id), **self.call_options()
            )
        ).sandbox

    async def delete_sandbox(self, sandbox_id: str) -> sandboxes_pb2.Sandbox:
        return (
            await self.sandboxes.DeleteSandbox(
                sandboxes_pb2.DeleteSandboxRequest(sandbox_id=sandbox_id), **self.call_options()
            )
        ).sandbox

    async def start_process(
        self, sandbox_id: str, command: list[str], *, cwd: str = "/data/workspace", timeout_seconds: int = 300
    ) -> guest.Process:
        return await self.processes.StartProcess(
            guest.StartProcessRequest(command=command, cwd=cwd, timeout={"seconds": timeout_seconds}),
            **self.call_options(sandbox_id),
        )

    async def get_process(self, sandbox_id: str, process_id: str) -> guest.Process:
        return await self.processes.GetProcess(
            guest.GetProcessRequest(process_id=process_id), **self.call_options(sandbox_id)
        )

    async def stream_output(self, sandbox_id: str, process_id: str) -> AsyncIterator[guest.ProcessOutput]:
        call = self.processes.StreamProcessOutput(
            guest.StreamProcessOutputRequest(process_id=process_id, follow=True), **self.call_options(sandbox_id)
        )
        try:
            async for event in call:
                yield event
        finally:
            call.cancel()

    async def upload(self, sandbox_id: str, local_path: str | Path, remote_path: str) -> int:
        async def chunks():
            source = await asyncio.to_thread(Path(local_path).open, "rb")
            try:
                # The first message also creates/truncates empty files.
                yield guest.WriteFileRequest(path=remote_path, mode=0o644)
                while chunk := await asyncio.to_thread(source.read, 256 << 10):
                    yield guest.WriteFileRequest(chunk=chunk)
            finally:
                await asyncio.to_thread(source.close)

        result = await self.files.WriteFile(chunks(), **self.call_options(sandbox_id))
        return result.bytes_written

    async def download(self, sandbox_id: str, remote_path: str, local_path: str | Path) -> int:
        """Replace the destination only after a complete successful transfer."""
        target = Path(local_path)
        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        call = self.files.ReadFile(guest.ReadFileRequest(path=remote_path), **self.call_options(sandbox_id))
        temporary = None
        size = 0
        try:
            with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as output:
                temporary = Path(output.name)
                async for response in call:
                    await asyncio.to_thread(output.write, response.chunk)
                    size += len(response.chunk)
            os.replace(temporary, target)
            return size
        finally:
            call.cancel()
            if temporary is not None:
                temporary.unlink(missing_ok=True)
