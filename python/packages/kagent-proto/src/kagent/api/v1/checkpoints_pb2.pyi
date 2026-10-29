import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from kagent.api.v1 import common_pb2 as _common_pb2
from kagent.api.v1 import sessions_pb2 as _sessions_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CheckpointState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CHECKPOINT_STATE_UNSPECIFIED: _ClassVar[CheckpointState]
    CHECKPOINT_STATE_CREATING: _ClassVar[CheckpointState]
    CHECKPOINT_STATE_READY: _ClassVar[CheckpointState]
    CHECKPOINT_STATE_FAILED: _ClassVar[CheckpointState]
    CHECKPOINT_STATE_DELETING: _ClassVar[CheckpointState]
CHECKPOINT_STATE_UNSPECIFIED: CheckpointState
CHECKPOINT_STATE_CREATING: CheckpointState
CHECKPOINT_STATE_READY: CheckpointState
CHECKPOINT_STATE_FAILED: CheckpointState
CHECKPOINT_STATE_DELETING: CheckpointState

class Checkpoint(_message.Message):
    __slots__ = ("id", "session_id", "head_task_id", "history_sequence", "state", "created_at", "failure", "name")
    ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    HEAD_TASK_ID_FIELD_NUMBER: _ClassVar[int]
    HISTORY_SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    id: str
    session_id: str
    head_task_id: str
    history_sequence: int
    state: CheckpointState
    created_at: _timestamp_pb2.Timestamp
    failure: _sessions_pb2.Failure
    name: str
    def __init__(self, id: _Optional[str] = ..., session_id: _Optional[str] = ..., head_task_id: _Optional[str] = ..., history_sequence: _Optional[int] = ..., state: _Optional[_Union[CheckpointState, str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., failure: _Optional[_Union[_sessions_pb2.Failure, _Mapping]] = ..., name: _Optional[str] = ...) -> None: ...

class CreateCheckpointRequest(_message.Message):
    __slots__ = ("session_id", "request_id", "expected_head_task_id")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_HEAD_TASK_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    request_id: str
    expected_head_task_id: str
    def __init__(self, session_id: _Optional[str] = ..., request_id: _Optional[str] = ..., expected_head_task_id: _Optional[str] = ...) -> None: ...

class CreateCheckpointResponse(_message.Message):
    __slots__ = ("checkpoint",)
    CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    checkpoint: Checkpoint
    def __init__(self, checkpoint: _Optional[_Union[Checkpoint, _Mapping]] = ...) -> None: ...

class GetCheckpointRequest(_message.Message):
    __slots__ = ("checkpoint_id",)
    CHECKPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    checkpoint_id: str
    def __init__(self, checkpoint_id: _Optional[str] = ...) -> None: ...

class GetCheckpointResponse(_message.Message):
    __slots__ = ("checkpoint",)
    CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    checkpoint: Checkpoint
    def __init__(self, checkpoint: _Optional[_Union[Checkpoint, _Mapping]] = ...) -> None: ...

class ListCheckpointsRequest(_message.Message):
    __slots__ = ("session_id", "page")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    page: _common_pb2.PageRequest
    def __init__(self, session_id: _Optional[str] = ..., page: _Optional[_Union[_common_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListCheckpointsResponse(_message.Message):
    __slots__ = ("checkpoints", "page")
    CHECKPOINTS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    checkpoints: _containers.RepeatedCompositeFieldContainer[Checkpoint]
    page: _common_pb2.PageResponse
    def __init__(self, checkpoints: _Optional[_Iterable[_Union[Checkpoint, _Mapping]]] = ..., page: _Optional[_Union[_common_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class DeleteCheckpointRequest(_message.Message):
    __slots__ = ("checkpoint_id",)
    CHECKPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    checkpoint_id: str
    def __init__(self, checkpoint_id: _Optional[str] = ...) -> None: ...

class DeleteCheckpointResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ForkSessionRequest(_message.Message):
    __slots__ = ("checkpoint_id", "request_id")
    CHECKPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    checkpoint_id: str
    request_id: str
    def __init__(self, checkpoint_id: _Optional[str] = ..., request_id: _Optional[str] = ...) -> None: ...

class ForkSessionResponse(_message.Message):
    __slots__ = ("session",)
    SESSION_FIELD_NUMBER: _ClassVar[int]
    session: _sessions_pb2.Session
    def __init__(self, session: _Optional[_Union[_sessions_pb2.Session, _Mapping]] = ...) -> None: ...

class UpdateCheckpointNameRequest(_message.Message):
    __slots__ = ("checkpoint_id", "name")
    CHECKPOINT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    checkpoint_id: str
    name: str
    def __init__(self, checkpoint_id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class UpdateCheckpointNameResponse(_message.Message):
    __slots__ = ("checkpoint",)
    CHECKPOINT_FIELD_NUMBER: _ClassVar[int]
    checkpoint: Checkpoint
    def __init__(self, checkpoint: _Optional[_Union[Checkpoint, _Mapping]] = ...) -> None: ...
