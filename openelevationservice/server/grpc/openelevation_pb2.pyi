from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LatLon(_message.Message):
    __slots__ = ("lat", "lon")
    LAT_FIELD_NUMBER: _ClassVar[int]
    LON_FIELD_NUMBER: _ClassVar[int]
    lat: float
    lon: float
    def __init__(self, lat: _Optional[float] = ..., lon: _Optional[float] = ...) -> None: ...

class Elevation(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class LineRequest(_message.Message):
    __slots__ = ("start", "end")
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    start: LatLon
    end: LatLon
    def __init__(self, start: _Optional[_Union[LatLon, _Mapping]] = ..., end: _Optional[_Union[LatLon, _Mapping]] = ...) -> None: ...

class LatLonElevation(_message.Message):
    __slots__ = ("lat", "lon", "elevation")
    LAT_FIELD_NUMBER: _ClassVar[int]
    LON_FIELD_NUMBER: _ClassVar[int]
    ELEVATION_FIELD_NUMBER: _ClassVar[int]
    lat: float
    lon: float
    elevation: int
    def __init__(self, lat: _Optional[float] = ..., lon: _Optional[float] = ..., elevation: _Optional[int] = ...) -> None: ...

class LineResponse(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[LatLonElevation]
    def __init__(self, points: _Optional[_Iterable[_Union[LatLonElevation, _Mapping]]] = ...) -> None: ...

class AreaRequest(_message.Message):
    __slots__ = ("bottomLeft", "topRight", "useOldOES")
    BOTTOMLEFT_FIELD_NUMBER: _ClassVar[int]
    TOPRIGHT_FIELD_NUMBER: _ClassVar[int]
    USEOLDOES_FIELD_NUMBER: _ClassVar[int]
    bottomLeft: LatLon
    topRight: LatLon
    useOldOES: int
    def __init__(self, bottomLeft: _Optional[_Union[LatLon, _Mapping]] = ..., topRight: _Optional[_Union[LatLon, _Mapping]] = ..., useOldOES: _Optional[int] = ...) -> None: ...

class AreaPointsResponse(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[LatLonElevation]
    def __init__(self, points: _Optional[_Iterable[_Union[LatLonElevation, _Mapping]]] = ...) -> None: ...

class LineString(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[LatLon]
    def __init__(self, points: _Optional[_Iterable[_Union[LatLon, _Mapping]]] = ...) -> None: ...

class Area(_message.Message):
    __slots__ = ("boundaries",)
    BOUNDARIES_FIELD_NUMBER: _ClassVar[int]
    boundaries: _containers.RepeatedCompositeFieldContainer[LineString]
    def __init__(self, boundaries: _Optional[_Iterable[_Union[LineString, _Mapping]]] = ...) -> None: ...

class UnitedArea(_message.Message):
    __slots__ = ("baseElevation", "area")
    BASEELEVATION_FIELD_NUMBER: _ClassVar[int]
    AREA_FIELD_NUMBER: _ClassVar[int]
    baseElevation: int
    area: Area
    def __init__(self, baseElevation: _Optional[int] = ..., area: _Optional[_Union[Area, _Mapping]] = ...) -> None: ...

class AreaRangesResponse(_message.Message):
    __slots__ = ("unions", "minElevation", "maxElevation", "avgElevation")
    UNIONS_FIELD_NUMBER: _ClassVar[int]
    MINELEVATION_FIELD_NUMBER: _ClassVar[int]
    MAXELEVATION_FIELD_NUMBER: _ClassVar[int]
    AVGELEVATION_FIELD_NUMBER: _ClassVar[int]
    unions: _containers.RepeatedCompositeFieldContainer[UnitedArea]
    minElevation: int
    maxElevation: int
    avgElevation: float
    def __init__(self, unions: _Optional[_Iterable[_Union[UnitedArea, _Mapping]]] = ..., minElevation: _Optional[int] = ..., maxElevation: _Optional[int] = ..., avgElevation: _Optional[float] = ...) -> None: ...
