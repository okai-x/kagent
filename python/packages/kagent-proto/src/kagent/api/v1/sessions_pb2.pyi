import datetime

from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from kagent.api.v1 import common_pb2 as _common_pb2
from kagent.api.v1 import runtime_pb2 as _runtime_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReadWorkspaceFileRequest(_message.Message):
    __slots__ = ("session_id", "path", "expected_head_task_id")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_HEAD_TASK_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    path: str
    expected_head_task_id: str
    def __init__(self, session_id: _Optional[str] = ..., path: _Optional[str] = ..., expected_head_task_id: _Optional[str] = ...) -> None: ...

class ReadWorkspaceFileResponse(_message.Message):
    __slots__ = ("chunk",)
    CHUNK_FIELD_NUMBER: _ClassVar[int]
    chunk: bytes
    def __init__(self, chunk: _Optional[bytes] = ...) -> None: ...

class Failure(_message.Message):
    __slots__ = ("reason", "message")
    REASON_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    reason: str
    message: str
    def __init__(self, reason: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class Session(_message.Message):
    __slots__ = ("id", "creator", "agent", "prepared_revision", "a2a_authority", "state", "operation", "failure", "created_at", "updated_at", "name", "context_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    CREATOR_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    PREPARED_REVISION_FIELD_NUMBER: _ClassVar[int]
    A2A_AUTHORITY_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    FAILURE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    creator: str
    agent: _common_pb2.ResourceReference
    prepared_revision: str
    a2a_authority: str
    state: _runtime_pb2.RuntimeState
    operation: _runtime_pb2.RuntimeOperation
    failure: Failure
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    name: str
    context_id: str
    def __init__(self, id: _Optional[str] = ..., creator: _Optional[str] = ..., agent: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ..., prepared_revision: _Optional[str] = ..., a2a_authority: _Optional[str] = ..., state: _Optional[_Union[_runtime_pb2.RuntimeState, str]] = ..., operation: _Optional[_Union[_runtime_pb2.RuntimeOperation, str]] = ..., failure: _Optional[_Union[Failure, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., name: _Optional[str] = ..., context_id: _Optional[str] = ...) -> None: ...

class CreateSessionRequest(_message.Message):
    __slots__ = ("agent", "request_id", "name")
    AGENT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    agent: _common_pb2.ResourceReference
    request_id: str
    name: str
    def __init__(self, agent: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ..., request_id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class CreateSessionResponse(_message.Message):
    __slots__ = ("session",)
    SESSION_FIELD_NUMBER: _ClassVar[int]
    session: Session
    def __init__(self, session: _Optional[_Union[Session, _Mapping]] = ...) -> None: ...

class GetSessionRequest(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    def __init__(self, session_id: _Optional[str] = ...) -> None: ...

class GetSessionResponse(_message.Message):
    __slots__ = ("session",)
    SESSION_FIELD_NUMBER: _ClassVar[int]
    session: Session
    def __init__(self, session: _Optional[_Union[Session, _Mapping]] = ...) -> None: ...

class ListSessionsRequest(_message.Message):
    __slots__ = ("all_creators", "page", "agent")
    ALL_CREATORS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    AGENT_FIELD_NUMBER: _ClassVar[int]
    all_creators: bool
    page: _common_pb2.PageRequest
    agent: _common_pb2.ResourceReference
    def __init__(self, all_creators: _Optional[bool] = ..., page: _Optional[_Union[_common_pb2.PageRequest, _Mapping]] = ..., agent: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ...) -> None: ...

class ListSessionsResponse(_message.Message):
    __slots__ = ("sessions", "page")
    SESSIONS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    sessions: _containers.RepeatedCompositeFieldContainer[Session]
    page: _common_pb2.PageResponse
    def __init__(self, sessions: _Optional[_Iterable[_Union[Session, _Mapping]]] = ..., page: _Optional[_Union[_common_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class UpdateSessionNameRequest(_message.Message):
    __slots__ = ("session_id", "name")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    name: str
    def __init__(self, session_id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class UpdateSessionNameResponse(_message.Message):
    __slots__ = ("session",)
    SESSION_FIELD_NUMBER: _ClassVar[int]
    session: Session
    def __init__(self, session: _Optional[_Union[Session, _Mapping]] = ...) -> None: ...

class DeleteSessionRequest(_message.Message):
    __slots__ = ("session_id",)
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    def __init__(self, session_id: _Optional[str] = ...) -> None: ...

class DeleteSessionResponse(_message.Message):
    __slots__ = ("session",)
    SESSION_FIELD_NUMBER: _ClassVar[int]
    session: Session
    def __init__(self, session: _Optional[_Union[Session, _Mapping]] = ...) -> None: ...

class SessionShare(_message.Message):
    __slots__ = ("id", "session_id", "created_at", "expires_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    session_id: str
    created_at: _timestamp_pb2.Timestamp
    expires_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., session_id: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CreateSessionShareRequest(_message.Message):
    __slots__ = ("session_id", "ttl")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    ttl: _duration_pb2.Duration
    def __init__(self, session_id: _Optional[str] = ..., ttl: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...

class CreateSessionShareResponse(_message.Message):
    __slots__ = ("share", "token")
    SHARE_FIELD_NUMBER: _ClassVar[int]
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    share: SessionShare
    token: str
    def __init__(self, share: _Optional[_Union[SessionShare, _Mapping]] = ..., token: _Optional[str] = ...) -> None: ...

class ListSessionSharesRequest(_message.Message):
    __slots__ = ("session_id", "page")
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    session_id: str
    page: _common_pb2.PageRequest
    def __init__(self, session_id: _Optional[str] = ..., page: _Optional[_Union[_common_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListSessionSharesResponse(_message.Message):
    __slots__ = ("shares", "page")
    SHARES_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    shares: _containers.RepeatedCompositeFieldContainer[SessionShare]
    page: _common_pb2.PageResponse
    def __init__(self, shares: _Optional[_Iterable[_Union[SessionShare, _Mapping]]] = ..., page: _Optional[_Union[_common_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class RevokeSessionShareRequest(_message.Message):
    __slots__ = ("share_id",)
    SHARE_ID_FIELD_NUMBER: _ClassVar[int]
    share_id: str
    def __init__(self, share_id: _Optional[str] = ...) -> None: ...

class RevokeSessionShareResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
