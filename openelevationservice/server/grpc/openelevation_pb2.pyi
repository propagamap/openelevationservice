from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Area(_message.Message):
    __slots__ = ["boundaries"]
    BOUNDARIES_FIELD_NUMBER: _ClassVar[int]
    boundaries: _containers.RepeatedCompositeFieldContainer[LineString]
    def __init__(self, boundaries: _Optional[_Iterable[_Union[LineString, _Mapping]]] = ...) -> None: ...

class AreaPointsResponse(_message.Message):
    __slots__ = ["points"]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[LatLonElevation]
    def __init__(self, points: _Optional[_Iterable[_Union[LatLonElevation, _Mapping]]] = ...) -> None: ...

class AreaRangesResponse(_message.Message):
    __slots__ = ["maxElevation", "minElevation", "unions"]
    MAXELEVATION_FIELD_NUMBER: _ClassVar[int]
    MINELEVATION_FIELD_NUMBER: _ClassVar[int]
    UNIONS_FIELD_NUMBER: _ClassVar[int]
    maxElevation: int
    minElevation: int
    unions: _containers.RepeatedCompositeFieldContainer[UnitedArea]
    def __init__(self, unions: _Optional[_Iterable[_Union[UnitedArea, _Mapping]]] = ..., minElevation: _Optional[int] = ..., maxElevation: _Optional[int] = ...) -> None: ...

class AreaRequest(_message.Message):
    __slots__ = ["bottomLeft", "topRight"]
    BOTTOMLEFT_FIELD_NUMBER: _ClassVar[int]
    TOPRIGHT_FIELD_NUMBER: _ClassVar[int]
    bottomLeft: LatLon
    topRight: LatLon
    def __init__(self, bottomLeft: _Optional[_Union[LatLon, _Mapping]] = ..., topRight: _Optional[_Union[LatLon, _Mapping]] = ...) -> None: ...

class Elevation(_message.Message):
    __slots__ = ["value"]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: int
    def __init__(self, value: _Optional[int] = ...) -> None: ...

class LatLon(_message.Message):
    __slots__ = ["lat", "lon"]
    LAT_FIELD_NUMBER: _ClassVar[int]
    LON_FIELD_NUMBER: _ClassVar[int]
    lat: float
    lon: float
    def __init__(self, lat: _Optional[float] = ..., lon: _Optional[float] = ...) -> None: ...

class LatLonElevation(_message.Message):
    __slots__ = ["elevation", "lat", "lon"]
    ELEVATION_FIELD_NUMBER: _ClassVar[int]
    LAT_FIELD_NUMBER: _ClassVar[int]
    LON_FIELD_NUMBER: _ClassVar[int]
    elevation: int
    lat: float
    lon: float
    def __init__(self, lat: _Optional[float] = ..., lon: _Optional[float] = ..., elevation: _Optional[int] = ...) -> None: ...

class LineRequest(_message.Message):
    __slots__ = ["end", "start"]
    END_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    end: LatLon
    start: LatLon
    def __init__(self, start: _Optional[_Union[LatLon, _Mapping]] = ..., end: _Optional[_Union[LatLon, _Mapping]] = ...) -> None: ...

class LineResponse(_message.Message):
    __slots__ = ["points"]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[LatLonElevation]
    def __init__(self, points: _Optional[_Iterable[_Union[LatLonElevation, _Mapping]]] = ...) -> None: ...

class LineString(_message.Message):
    __slots__ = ["points"]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[LatLon]
    def __init__(self, points: _Optional[_Iterable[_Union[LatLon, _Mapping]]] = ...) -> None: ...

class UnitedArea(_message.Message):
    __slots__ = ["area", "baseElevation"]
    AREA_FIELD_NUMBER: _ClassVar[int]
    BASEELEVATION_FIELD_NUMBER: _ClassVar[int]
    area: Area
    baseElevation: int
    def __init__(self, baseElevation: _Optional[int] = ..., area: _Optional[_Union[Area, _Mapping]] = ...) -> None: ...
