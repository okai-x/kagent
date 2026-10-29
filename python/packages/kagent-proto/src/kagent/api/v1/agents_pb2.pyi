from buf.validate import validate_pb2 as _validate_pb2
from kagent.api.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Agent(_message.Message):
    __slots__ = ("ref", "resource")
    REF_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    resource: _common_pb2.StructuredObject
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ..., resource: _Optional[_Union[_common_pb2.StructuredObject, _Mapping]] = ...) -> None: ...

class ListAgentsRequest(_message.Message):
    __slots__ = ("namespace", "page")
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    page: _common_pb2.PageRequest
    def __init__(self, namespace: _Optional[str] = ..., page: _Optional[_Union[_common_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListAgentsResponse(_message.Message):
    __slots__ = ("agents", "page")
    AGENTS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    agents: _containers.RepeatedCompositeFieldContainer[Agent]
    page: _common_pb2.PageResponse
    def __init__(self, agents: _Optional[_Iterable[_Union[Agent, _Mapping]]] = ..., page: _Optional[_Union[_common_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class GetAgentRequest(_message.Message):
    __slots__ = ("ref",)
    REF_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ...) -> None: ...

class GetAgentResponse(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: Agent
    def __init__(self, agent: _Optional[_Union[Agent, _Mapping]] = ...) -> None: ...

class CreateAgentRequest(_message.Message):
    __slots__ = ("ref", "resource")
    REF_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    resource: _common_pb2.StructuredObject
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ..., resource: _Optional[_Union[_common_pb2.StructuredObject, _Mapping]] = ...) -> None: ...

class CreateAgentResponse(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: Agent
    def __init__(self, agent: _Optional[_Union[Agent, _Mapping]] = ...) -> None: ...

class UpdateAgentRequest(_message.Message):
    __slots__ = ("ref", "resource")
    REF_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    resource: _common_pb2.StructuredObject
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ..., resource: _Optional[_Union[_common_pb2.StructuredObject, _Mapping]] = ...) -> None: ...

class UpdateAgentResponse(_message.Message):
    __slots__ = ("agent",)
    AGENT_FIELD_NUMBER: _ClassVar[int]
    agent: Agent
    def __init__(self, agent: _Optional[_Union[Agent, _Mapping]] = ...) -> None: ...

class DeleteAgentRequest(_message.Message):
    __slots__ = ("ref",)
    REF_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ...) -> None: ...

class DeleteAgentResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
