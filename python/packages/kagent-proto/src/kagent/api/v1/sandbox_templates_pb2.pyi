from buf.validate import validate_pb2 as _validate_pb2
from kagent.api.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SandboxTemplate(_message.Message):
    __slots__ = ("ref", "resource", "workload_image")
    REF_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    WORKLOAD_IMAGE_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    resource: _common_pb2.StructuredObject
    workload_image: str
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ..., resource: _Optional[_Union[_common_pb2.StructuredObject, _Mapping]] = ..., workload_image: _Optional[str] = ...) -> None: ...

class ListSandboxTemplatesRequest(_message.Message):
    __slots__ = ("namespace",)
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    namespace: str
    def __init__(self, namespace: _Optional[str] = ...) -> None: ...

class ListSandboxTemplatesResponse(_message.Message):
    __slots__ = ("sandbox_templates",)
    SANDBOX_TEMPLATES_FIELD_NUMBER: _ClassVar[int]
    sandbox_templates: _containers.RepeatedCompositeFieldContainer[SandboxTemplate]
    def __init__(self, sandbox_templates: _Optional[_Iterable[_Union[SandboxTemplate, _Mapping]]] = ...) -> None: ...

class CreateSandboxTemplateRequest(_message.Message):
    __slots__ = ("ref", "resource")
    REF_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    resource: _common_pb2.StructuredObject
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ..., resource: _Optional[_Union[_common_pb2.StructuredObject, _Mapping]] = ...) -> None: ...

class CreateSandboxTemplateResponse(_message.Message):
    __slots__ = ("sandbox_template",)
    SANDBOX_TEMPLATE_FIELD_NUMBER: _ClassVar[int]
    sandbox_template: SandboxTemplate
    def __init__(self, sandbox_template: _Optional[_Union[SandboxTemplate, _Mapping]] = ...) -> None: ...

class DeleteSandboxTemplateRequest(_message.Message):
    __slots__ = ("ref",)
    REF_FIELD_NUMBER: _ClassVar[int]
    ref: _common_pb2.ResourceReference
    def __init__(self, ref: _Optional[_Union[_common_pb2.ResourceReference, _Mapping]] = ...) -> None: ...

class DeleteSandboxTemplateResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
