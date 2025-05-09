# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: openelevationservice/server/grpc/openelevation.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n4openelevationservice/server/grpc/openelevation.proto\x12\npropagamap\"\"\n\x06LatLon\x12\x0b\n\x03lat\x18\x01 \x01(\x01\x12\x0b\n\x03lon\x18\x02 \x01(\x01\"\x1a\n\tElevation\x12\r\n\x05value\x18\x01 \x01(\x05\"Q\n\x0bLineRequest\x12!\n\x05start\x18\x01 \x01(\x0b\x32\x12.propagamap.LatLon\x12\x1f\n\x03\x65nd\x18\x02 \x01(\x0b\x32\x12.propagamap.LatLon\">\n\x0fLatLonElevation\x12\x0b\n\x03lat\x18\x01 \x01(\x01\x12\x0b\n\x03lon\x18\x02 \x01(\x01\x12\x11\n\televation\x18\x03 \x01(\x05\";\n\x0cLineResponse\x12+\n\x06points\x18\x01 \x03(\x0b\x32\x1b.propagamap.LatLonElevation\"[\n\x0b\x41reaRequest\x12&\n\nbottomLeft\x18\x01 \x01(\x0b\x32\x12.propagamap.LatLon\x12$\n\x08topRight\x18\x02 \x01(\x0b\x32\x12.propagamap.LatLon\"A\n\x12\x41reaPointsResponse\x12+\n\x06points\x18\x01 \x03(\x0b\x32\x1b.propagamap.LatLonElevation\"0\n\nLineString\x12\"\n\x06points\x18\x01 \x03(\x0b\x32\x12.propagamap.LatLon\"2\n\x04\x41rea\x12*\n\nboundaries\x18\x01 \x03(\x0b\x32\x16.propagamap.LineString\"C\n\nUnitedArea\x12\x15\n\rbaseElevation\x18\x01 \x01(\x05\x12\x1e\n\x04\x61rea\x18\x02 \x01(\x0b\x32\x10.propagamap.Area\"~\n\x12\x41reaRangesResponse\x12&\n\x06unions\x18\x01 \x03(\x0b\x32\x16.propagamap.UnitedArea\x12\x14\n\x0cminElevation\x18\x02 \x01(\x05\x12\x14\n\x0cmaxElevation\x18\x03 \x01(\x05\x12\x14\n\x0c\x61vgElevation\x18\x04 \x01(\x02\x32\xb0\x02\n\rOpenElevation\x12;\n\x0ePointElevation\x12\x12.propagamap.LatLon\x1a\x15.propagamap.Elevation\x12\x42\n\rLineElevation\x12\x17.propagamap.LineRequest\x1a\x18.propagamap.LineResponse\x12N\n\x13\x41reaPointsElevation\x12\x17.propagamap.AreaRequest\x1a\x1e.propagamap.AreaPointsResponse\x12N\n\x13\x41reaRangesElevation\x12\x17.propagamap.AreaRequest\x1a\x1e.propagamap.AreaRangesResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'openelevationservice.server.grpc.openelevation_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_LATLON']._serialized_start=68
  _globals['_LATLON']._serialized_end=102
  _globals['_ELEVATION']._serialized_start=104
  _globals['_ELEVATION']._serialized_end=130
  _globals['_LINEREQUEST']._serialized_start=132
  _globals['_LINEREQUEST']._serialized_end=213
  _globals['_LATLONELEVATION']._serialized_start=215
  _globals['_LATLONELEVATION']._serialized_end=277
  _globals['_LINERESPONSE']._serialized_start=279
  _globals['_LINERESPONSE']._serialized_end=338
  _globals['_AREAREQUEST']._serialized_start=340
  _globals['_AREAREQUEST']._serialized_end=431
  _globals['_AREAPOINTSRESPONSE']._serialized_start=433
  _globals['_AREAPOINTSRESPONSE']._serialized_end=498
  _globals['_LINESTRING']._serialized_start=500
  _globals['_LINESTRING']._serialized_end=548
  _globals['_AREA']._serialized_start=550
  _globals['_AREA']._serialized_end=600
  _globals['_UNITEDAREA']._serialized_start=602
  _globals['_UNITEDAREA']._serialized_end=669
  _globals['_AREARANGESRESPONSE']._serialized_start=671
  _globals['_AREARANGESRESPONSE']._serialized_end=797
  _globals['_OPENELEVATION']._serialized_start=800
  _globals['_OPENELEVATION']._serialized_end=1104
# @@protoc_insertion_point(module_scope)
