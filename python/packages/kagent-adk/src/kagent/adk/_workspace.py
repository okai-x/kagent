"""Grant-authorized file downloads; this service does not add model tools."""

import asyncio
import errno
import time
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import TypeVar

import grpc
from kagent.core import AsyncControllerClient
from kagent.runtime.v1 import workspace_pb2, workspace_pb2_grpc

from ._workspace_file import (
    UnsafeWorkspaceFile,
    WorkspaceFile,
    WorkspaceFileChanged,
    WorkspaceFileTooLarge,
    relative_parts,
)

_T = TypeVar("_T")


async def _file_operation(operation: Callable[[], _T]) -> _T:
    # Join the worker before closing its descriptor, including on RPC cancellation.
    pending = asyncio.create_task(asyncio.to_thread(operation))
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(pending)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
            if pending.cancelled():
                raise cancellation from None
        except Exception as error:
            if cancellation is not None:
                raise cancellation from error
            raise
        else:
            if cancellation is not None:
                raise cancellation
            return result


class WorkspaceService(workspace_pb2_grpc.WorkspaceServiceServicer):
    def __init__(self, controller: AsyncControllerClient, root: Path = Path("/data/workspace")) -> None:
        if not root.is_absolute():
            raise ValueError("workspace root must be absolute")
        self.controller = controller
        self.root = root

    async def ReadFile(
        self, request: workspace_pb2.ReadFileRequest, context: grpc.aio.ServicerContext
    ) -> AsyncIterator[workspace_pb2.ReadFileResponse]:
        try:
            parts = relative_parts(request.path)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid workspace path")
        try:
            grant = await self.controller.workspace_access_service.AuthorizeRead(
                workspace_pb2.AuthorizeReadRequest(grant_id=request.grant_id, path=request.path),
                **await self.controller.call_options(),
            )
        except grpc.aio.AioRpcError as error:
            await context.abort(error.code(), "workspace read authorization failed")
        expires_at = grant.expires_at.seconds + grant.expires_at.nanos / 1e9
        reader = WorkspaceFile(self.root, parts)
        try:
            await self._check_expiry(expires_at, context)
            await _file_operation(reader.open)
            while True:
                await self._check_expiry(expires_at, context)
                chunk = await _file_operation(reader.read)
                await self._check_expiry(expires_at, context)
                if not chunk:
                    return
                yield workspace_pb2.ReadFileResponse(chunk=chunk)
        except WorkspaceFileTooLarge:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "workspace file exceeds 64 MiB")
        except WorkspaceFileChanged:
            await context.abort(grpc.StatusCode.ABORTED, "workspace file changed during download")
        except UnsafeWorkspaceFile:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "workspace path is not a regular file")
        except OSError as error:
            await context.abort(_file_error_code(error), "workspace file could not be read")
        finally:
            reader.close()

    @staticmethod
    async def _check_expiry(expires_at: float, context: grpc.aio.ServicerContext) -> None:
        if expires_at <= time.time():
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "workspace read grant expired")


def _file_error_code(error: OSError) -> grpc.StatusCode:
    if error.errno == errno.ENOENT:
        return grpc.StatusCode.NOT_FOUND
    if error.errno in (errno.ELOOP, errno.ENOTDIR):
        return grpc.StatusCode.FAILED_PRECONDITION
    if error.errno in (errno.EACCES, errno.EPERM):
        return grpc.StatusCode.PERMISSION_DENIED
    return grpc.StatusCode.INTERNAL
