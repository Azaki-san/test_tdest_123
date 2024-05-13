from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Mark(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MARK_NOUGHT: _ClassVar[Mark]
    MARK_CROSS: _ClassVar[Mark]
MARK_NOUGHT: Mark
MARK_CROSS: Mark

class Move(_message.Message):
    __slots__ = ("mark", "cell")
    MARK_FIELD_NUMBER: _ClassVar[int]
    CELL_FIELD_NUMBER: _ClassVar[int]
    mark: Mark
    cell: int
    def __init__(self, mark: _Optional[_Union[Mark, str]] = ..., cell: _Optional[int] = ...) -> None: ...

class Game(_message.Message):
    __slots__ = ("id", "is_finished", "winner", "turn", "moves")
    ID_FIELD_NUMBER: _ClassVar[int]
    IS_FINISHED_FIELD_NUMBER: _ClassVar[int]
    WINNER_FIELD_NUMBER: _ClassVar[int]
    TURN_FIELD_NUMBER: _ClassVar[int]
    MOVES_FIELD_NUMBER: _ClassVar[int]
    id: int
    is_finished: bool
    winner: Mark
    turn: Mark
    moves: _containers.RepeatedCompositeFieldContainer[Move]
    def __init__(self, id: _Optional[int] = ..., is_finished: bool = ..., winner: _Optional[_Union[Mark, str]] = ..., turn: _Optional[_Union[Mark, str]] = ..., moves: _Optional[_Iterable[_Union[Move, _Mapping]]] = ...) -> None: ...

class GetGameRequest(_message.Message):
    __slots__ = ("game_id",)
    GAME_ID_FIELD_NUMBER: _ClassVar[int]
    game_id: int
    def __init__(self, game_id: _Optional[int] = ...) -> None: ...

class MakeMoveRequest(_message.Message):
    __slots__ = ("game_id", "move")
    GAME_ID_FIELD_NUMBER: _ClassVar[int]
    MOVE_FIELD_NUMBER: _ClassVar[int]
    game_id: int
    move: Move
    def __init__(self, game_id: _Optional[int] = ..., move: _Optional[_Union[Move, _Mapping]] = ...) -> None: ...

class CreateGameRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
