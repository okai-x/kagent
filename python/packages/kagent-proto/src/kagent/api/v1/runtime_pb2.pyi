from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from typing import ClassVar as _ClassVar

DESCRIPTOR: _descriptor.FileDescriptor

class RuntimeState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUNTIME_STATE_UNSPECIFIED: _ClassVar[RuntimeState]
    RUNTIME_STATE_CREATING: _ClassVar[RuntimeState]
    RUNTIME_STATE_READY: _ClassVar[RuntimeState]
    RUNTIME_STATE_SUSPENDED: _ClassVar[RuntimeState]
    RUNTIME_STATE_FAILED: _ClassVar[RuntimeState]
    RUNTIME_STATE_DELETING: _ClassVar[RuntimeState]
    RUNTIME_STATE_DELETED: _ClassVar[RuntimeState]

class RuntimeOperation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUNTIME_OPERATION_UNSPECIFIED: _ClassVar[RuntimeOperation]
    RUNTIME_OPERATION_CREATE: _ClassVar[RuntimeOperation]
    RUNTIME_OPERATION_SUSPEND: _ClassVar[RuntimeOperation]
    RUNTIME_OPERATION_RESUME: _ClassVar[RuntimeOperation]
    RUNTIME_OPERATION_DELETE: _ClassVar[RuntimeOperation]
    RUNTIME_OPERATION_NONE: _ClassVar[RuntimeOperation]
RUNTIME_STATE_UNSPECIFIED: RuntimeState
RUNTIME_STATE_CREATING: RuntimeState
RUNTIME_STATE_READY: RuntimeState
RUNTIME_STATE_SUSPENDED: RuntimeState
RUNTIME_STATE_FAILED: RuntimeState
RUNTIME_STATE_DELETING: RuntimeState
RUNTIME_STATE_DELETED: RuntimeState
RUNTIME_OPERATION_UNSPECIFIED: RuntimeOperation
RUNTIME_OPERATION_CREATE: RuntimeOperation
RUNTIME_OPERATION_SUSPEND: RuntimeOperation
RUNTIME_OPERATION_RESUME: RuntimeOperation
RUNTIME_OPERATION_DELETE: RuntimeOperation
RUNTIME_OPERATION_NONE: RuntimeOperation
