"""Descriptor-relative reads for the private workspace download boundary."""

import os
import stat
from pathlib import Path

MAX_FILE_BYTES = 64 << 20
CHUNK_BYTES = 256 << 10
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC


class UnsafeWorkspaceFile(ValueError):
    pass


class WorkspaceFileTooLarge(ValueError):
    pass


class WorkspaceFileChanged(ValueError):
    pass


def relative_parts(path: str) -> list[str]:
    parts = path.split("/")
    if (
        len(path.encode("utf-8")) > 4096
        or "\x00" in path
        or "\\" in path
        or any(part in ("", ".", "..") for part in parts)
    ):
        raise ValueError("path must be a canonical relative workspace path")
    return parts


class WorkspaceFile:
    def __init__(self, root: Path, parts: list[str]) -> None:
        self.root = root
        self.parts = parts
        self.fd: int | None = None
        self.size = 0
        self.total = 0
        self._stamp: tuple[int, int, int, int, int] | None = None

    def open(self) -> None:
        directory = os.open("/", _DIRECTORY_FLAGS)
        try:
            for part in (*self.root.parts[1:], *self.parts[:-1]):
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=directory)
                os.close(directory)
                directory = child
            self.fd = os.open(self.parts[-1], _FILE_FLAGS, dir_fd=directory)
        finally:
            os.close(directory)
        info = os.fstat(self.fd)
        if not stat.S_ISREG(info.st_mode):
            raise UnsafeWorkspaceFile("workspace downloads require a regular file")
        if info.st_size > MAX_FILE_BYTES:
            raise WorkspaceFileTooLarge("workspace file exceeds the size limit")
        self.size = info.st_size
        self._stamp = _file_stamp(info)

    def read(self) -> bytes:
        if self.fd is None:
            raise RuntimeError("workspace file is not open")
        chunk = os.read(self.fd, min(CHUNK_BYTES, MAX_FILE_BYTES - self.total + 1))
        self.total += len(chunk)
        if self.total > MAX_FILE_BYTES:
            raise WorkspaceFileTooLarge("workspace file exceeds the size limit")
        if not chunk and (self.total != self.size or _file_stamp(os.fstat(self.fd)) != self._stamp):
            raise WorkspaceFileChanged("workspace file changed during download")
        return chunk

    def close(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def _file_stamp(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
