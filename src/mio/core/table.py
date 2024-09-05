import inspect
import pathlib

import numpy as np
import graphviper.utils.logger as logger

from mio.utilities import types
from dataclasses import dataclass


@dataclass(init=False)
class RecordDescription:
    names: list
    types: list
    nrecords: int


@dataclass(init=False)
class TableRecord:
    description: RecordDescription
    records: dict


def read_record_description(file_handle) -> RecordDescription:
    description = RecordDescription()

    description.names = []
    description.types = []

    file_handle.check_type()

    # Look through the records and sort them by type:
    description.nrecords = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    for i in range(description.nrecords):
        description.names.append(file_handle.string(size=types.FOUR_BYTES))

        raw_type = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)
        description.types.append(types.TYPE_LIST[raw_type])

        if types.TYPE_LIST[raw_type] in ('bool', 'int', 'uint', 'float', 'double', 'complex', 'dcomplex', 'string'):
            file_handle.string(size=types.FOUR_BYTES)

        elif types.TYPE_LIST[raw_type] == "table":
            file_handle.read(types.EIGHT_BYTES)

        elif types.TYPE_LIST[raw_type].startswith("array"):
            file_handle.position(size=types.FOUR_BYTES, dtype=np.int32)
            file_handle.read(types.FOUR_BYTES)

        elif types.TYPE_LIST[raw_type] == "record":
            read_record_description(file_handle)
            file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

        else:
            logger.debug(f"Not implemented: {types.TYPE_LIST[raw_type]}")

    return description


def read_record(file_handle):
    file_handle.check_type()

    record_object = TableRecord()
    description = read_record_description(file_handle)
    record_object.description = description
    record_object.records = {}

    # Yet another unknown read who's meaning is lost to lack of documentation.
    file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    for name, record_type in zip(description.names, description.types):
        if record_type == "bool":
            record_object.records[name] = file_handle.bool()

        elif record_type == "int":
            record_object.records[name] = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

        elif record_type == "uint":
            record_object.records[name] = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

        elif record_type == "float":
            record_object.records[name] = file_handle.float(size=types.FOUR_BYTES, dtype=np.float32)

        elif record_type == "double":
            record_object.records[name] = file_handle.float(size=types.EIGHT_BYTES, dtype=np.float64)

        elif record_type == "complex":
            record_object.records[name] = file_handle.float(size=types.EIGHT_BYTES, dtype=np.float64)

        elif record_type == "dcomplex":
            record_object.records[name] = file_handle.float(size=types.SIXTEEN_BYTES, dtype=np.float128)

        elif record_type == "string":
            record_object.records[name] = file_handle.string(size=types.FOUR_BYTES)

        elif record_type == "table":
            record_object.records[name] = str(
                pathlib.Path(file_handle.filename).resolve().joinpath(file_handle.string(size=types.FOUR_BYTES)))

        elif record_type == 'arrayint':
            record_object.records[name] = file_handle.array('int').astype('<i4')

        elif record_type == 'arrayuint':
            record_object.records[name] = file_handle.array('uint').astype('<u4')

        elif record_type == 'arrayfloat':
            record_object.records[name] = file_handle.array('float').astype('<f4')

        elif record_type == 'arraydouble':
            record_object.records[name] = file_handle.array('double').astype('<f8')

        elif record_type == 'arraycomplex':
            record_object.records[name] = file_handle.array('complex').astype('<c8')

        elif record_type == 'arraydcomplex':
            record_object.records[name] = file_handle.array('dcomplex').astype('<c16')

        elif record_type == 'arraystr':
            record_object.records[name] = file_handle.array('string')

        elif record_type == 'record':
            record_object.records[name] = read_record(file_handle)

        else:
            raise NotImplementedError(f"Support for type {record_type} not implemented")

    return record_object
