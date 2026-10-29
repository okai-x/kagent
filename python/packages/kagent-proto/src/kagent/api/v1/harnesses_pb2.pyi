from kagent.api.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Harness(_message.Message):
    __slots__ = ("ref", "resource", "runtime", "workload_image", "ready")
    REF_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    RUNTIME_FIELD_NUMBER: _ClassVar[int]
    WORKLOAD_IMAGE_FIELD_NUMBER: _ClassVar[int]
    READY_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    resource: _common_pb2.StructuredObject
    runtime: str
    workload_image: str
    ready: bool
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ..., resource: _Optional[_Union[_common_pb2.StructuredObject, _Mapping]] = ..., runtime: _Optional[str] = ..., workload_image: _Optional[str] = ..., ready: _Optional[bool] = ...) -> None: ...

class ListHarnessesRequest(_message.Message):
    __slots__ = ("namespace",)
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    def __init__(self, namespace: _Optional[str] = ...) -> None: ...

class ListHarnessesResponse(_message.Message):
    __slots__ = ("harnesses",)
    HARNESSES_FIELD_NUMBER: _ClassVar[int]
    harnesses: _containers.RepeatedCompositeFieldContainer[Harness]
    def __init__(self, harnesses: _Optional[_Iterable[_Union[Harness, _Mapping]]] = ...) -> None: ...

class CreateHarnessRequest(_message.Message):
    __slots__ = ("ref", "resource")
    REF_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    resource: _common_pb2.StructuredObject
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ..., resource: _Optional[_Union[_common_pb2.StructuredObject, _Mapping]] = ...) -> None: ...

class CreateHarnessResponse(_message.Message):
    __slots__ = ("harness",)
    HARNESS_FIELD_NUMBER: _ClassVar[int]
    harness: Harness
    def __init__(self, harness: _Optional[_Union[Harness, _Mapping]] = ...) -> None: ...

class DeleteHarnessRequest(_message.Message):
    __slots__ = ("ref",)
    REF_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ...) -> None: ...

class DeleteHarnessResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
