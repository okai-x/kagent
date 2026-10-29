import os

import pytest

from kagent.adk._workspace_file import (
    CHUNK_BYTES,
    MAX_FILE_BYTES,
    WorkspaceFile,
    WorkspaceFileChanged,
    WorkspaceFileTooLarge,
    relative_parts,
)


def test_parent_symlink_is_rejected(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_text("secret")
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "link").symlink_to(outside, target_is_directory=True)
    reader = WorkspaceFile(root, relative_parts("link/secret"))
    try:
        with pytest.raises(OSError):
            reader.open()
    finally:
        reader.close()


def test_workspace_root_symlink_is_rejected(tmp_path):
    root = tmp_path / "link"
    root.symlink_to(tmp_path, target_is_directory=True)
    reader = WorkspaceFile(root, ["result.glb"])
    try:
        with pytest.raises(OSError):
            reader.open()
    finally:
        reader.close()


def test_open_descriptor_is_not_redirected_by_path_replacement(tmp_path):
    path = tmp_path / "result.glb"
    path.write_bytes(b"safe")
    reader = WorkspaceFile(tmp_path, [path.name])
    try:
        reader.open()
        path.rename(tmp_path / "original")
        path.symlink_to("/etc/passwd")
        assert reader.read() == b"safe"
        with pytest.raises(WorkspaceFileChanged):
            reader.read()
    finally:
        reader.close()


def test_growth_past_limit_is_not_successful_eof(tmp_path):
    path = tmp_path / "result.glb"
    path.write_bytes(b"safe")
    reader = WorkspaceFile(tmp_path, [path.name])
    try:
        reader.open()
        with path.open("ab") as stream:
            stream.truncate(MAX_FILE_BYTES + 1)
        with pytest.raises(WorkspaceFileTooLarge):
            while reader.read():
                pass
    finally:
        reader.close()


def test_truncated_file_fails_instead_of_returning_partial_file(tmp_path):
    path = tmp_path / "result.glb"
    path.write_bytes(b"x" * CHUNK_BYTES * 2)
    reader = WorkspaceFile(tmp_path, [path.name])
    try:
        reader.open()
        assert len(reader.read()) == CHUNK_BYTES
        os.truncate(path, CHUNK_BYTES)
        with pytest.raises(WorkspaceFileChanged):
            reader.read()
    finally:
        reader.close()


def test_nested_regular_file_can_be_read(tmp_path):
    path = tmp_path / "nested" / "result.glb"
    path.parent.mkdir()
    path.write_bytes(b"glTF")
    reader = WorkspaceFile(tmp_path, relative_parts("nested/result.glb"))
    try:
        reader.open()
        assert reader.read() == b"glTF"
        assert reader.read() == b""
    finally:
        reader.close()
