from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class AppendEntriesArgs(_message.Message):
    __slots__ = ("leader_id", "leader_term", "committed_value", "uncommitted_value")
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    LEADER_TERM_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_VALUE_FIELD_NUMBER: _ClassVar[int]
    UNCOMMITTED_VALUE_FIELD_NUMBER: _ClassVar[int]
    leader_id: int
    leader_term: int
    committed_value: int
    uncommitted_value: int
    def __init__(self, leader_id: _Optional[int] = ..., leader_term: _Optional[int] = ..., committed_value: _Optional[int] = ..., uncommitted_value: _Optional[int] = ...) -> None: ...

class AppendEntriesResponse(_message.Message):
    __slots__ = ("term", "heartbeat_result")
    TERM_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_RESULT_FIELD_NUMBER: _ClassVar[int]
    term: int
    heartbeat_result: bool
    def __init__(self, term: _Optional[int] = ..., heartbeat_result: bool = ...) -> None: ...

class RequestVoteArgs(_message.Message):
    __slots__ = ("candidate_id", "candidate_term")
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_TERM_FIELD_NUMBER: _ClassVar[int]
    candidate_id: int
    candidate_term: int
    def __init__(self, candidate_id: _Optional[int] = ..., candidate_term: _Optional[int] = ...) -> None: ...

class RequestVoteResponse(_message.Message):
    __slots__ = ("term", "vote_result")
    TERM_FIELD_NUMBER: _ClassVar[int]
    VOTE_RESULT_FIELD_NUMBER: _ClassVar[int]
    term: int
    vote_result: bool
    def __init__(self, term: _Optional[int] = ..., vote_result: bool = ...) -> None: ...

class GetLeaderArgs(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetLeaderResponse(_message.Message):
    __slots__ = ("leader_id",)
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    leader_id: int
    def __init__(self, leader_id: _Optional[int] = ...) -> None: ...

class AddValueArgs(_message.Message):
    __slots__ = ("value_to_add",)
    VALUE_TO_ADD_FIELD_NUMBER: _ClassVar[int]
    value_to_add: int
    def __init__(self, value_to_add: _Optional[int] = ...) -> None: ...

class AddValueResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetValueArgs(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetValueResponse(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class SuspendArgs(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SuspendResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ResumeArgs(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ResumeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
