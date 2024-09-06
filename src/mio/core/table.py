import pathlib
from collections import OrderedDict
from typing import Union

from mio.core.binary import BinaryFileReader
import numpy as np
import graphviper.utils.logger as logger

from mio.utilities import types
from mio.managers import store

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


@dataclass(init=False)
class ColumnDescription:
    name: str
    type: str
    comment: str
    manager_group: str
    manager_type: str
    value_type: str
    option: int
    direct: bool
    undefined: bool
    fixed_shape: bool
    shape: list
    ndims: int
    max_length: int
    keywords: TableRecord


@dataclass(init=False)
class ColumnSet:
    nrows: int
    nrman: int
    nmanagers: int
    columns: dict
    data_managers: OrderedDict


@dataclass(init=False)
class ColumnData:
    version: int
    sequence_number: int
    shape: tuple


@dataclass(init=False)
class PlainColumn:
    name: str
    data: ColumnData


def build_array_column_data(file_handle) -> ColumnData:
    column_data = ColumnData()
    column_data.version = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)
    column_data.sequence_number = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    shape = file_handle.boolean()
    logger.debug(f"shape: {shape}")

    column_data.shape = file_handle.position(size=types.FOUR_BYTES, dtype=np.int32).tolist() if shape else tuple()

    return column_data


def build_scalar_column_data(file_handle) -> ColumnData:
    column_data = ColumnData()
    column_data.version = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)
    column_data.sequence_number = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    column_data.shape = tuple()

    return column_data


def build_plain_column(file_handle, ndims) -> PlainColumn:
    plain_column = PlainColumn()
    version = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)
    logger.warning(f"version: {version}")

    if version < 2:
        logger.error(f"Plain column support doesn't exist for version < 2")
        raise NotImplementedError

    plain_column.name = file_handle.string(size=types.FOUR_BYTES)
    plain_column.data = build_array_column_data(file_handle) if ndims != 0 else build_scalar_column_data(file_handle)

    logger.warning(f"name: {plain_column.name} ({ndims}): {plain_column.data}")

    return plain_column


def read_column_set(file_handle: BinaryFileReader, description: list[ColumnDescription]) -> ColumnSet:
    column_set = ColumnSet()

    version = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    # Haven't looked up why this is done ...
    version *= -1

    # Perhaps the total number of data rows?
    column_set.nrows = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    # No idea what this is yet, will update name when I figure it out.
    column_set.nrman = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    # This seems to be the number of data managers.
    column_set.nmanagers = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    data_manager_class = OrderedDict()

    for _ in range(column_set.nmanagers):
        name = file_handle.string(size=types.FOUR_BYTES)
        sequence_number = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

        if name in store.MANAGER_STORE.keys():
            data_manager_class[sequence_number] = store.MANAGER_STORE[name]

        else:
            logger.error(f"Data manager not supported: {name}")
            raise NotImplementedError()

    # It is really strange that they laid the binary file out this way. Seems like it would have beeb better
    # to read the data managers here instead of switching to plain column reads.
    column_set.columns = [build_plain_column(file_handle, entry.ndims) for entry in description]

    # Read magic code (again) and a length
    file_handle.read(types.EIGHT_BYTES)

    column_set.data_manager_class = OrderedDict()

    for sequence_number in data_manager_class:
        column_set.data_managers[sequence_number] = data_manager_class[sequence_number].read(types.EIGHT_BYTES)

    return column_set


def read_column_description(file_handle) -> ColumnDescription:
    # Instantiate column description structure
    column_description = ColumnDescription()

    # Another unknown read
    file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    # Get column meta information
    column_description.type = file_handle.string(size=types.FOUR_BYTES)
    column_description.version = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    # Check that the column type is supported
    if not column_description.type.startswith(
            ("ScalarColumnDesc", "ScalarRecordColumnDesc", "ArrayColumnDesc")) or column_description.version != 1:
        raise NotImplementedError(
            f"Support for {column_description.type} not available in {column_description.version}")

    column_description.name = file_handle.string(size=types.FOUR_BYTES)
    column_description.comment = file_handle.string(size=types.FOUR_BYTES)
    column_description.manager_type = file_handle.string(size=types.FOUR_BYTES).replace("Shape", "Cell")
    column_description.manager_group = file_handle.string(size=types.FOUR_BYTES)
    column_description.value_type = types.TYPE_LIST[file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)]

    column_description.option = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    # Here we want to determine which bit is set
    #   option = 0x1010 for instance
    #   direct >> 3 ==> 0x1 so direct is then
    #   direct = 0x1 == 0x1 ==> True
    column_description.direct = (column_description.option >> 3) == types.ONE_BYTE
    column_description.undefined = (column_description.option >> 2) == types.ONE_BYTE
    column_description.fixed_shape = (column_description.option >> 1) == types.ONE_BYTE

    # Set default value for shape
    column_description.shape = None

    # Get number of dimensions
    column_description.ndims = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    if column_description.ndims != 0:
        column_description.shape = file_handle.position(size=types.FOUR_BYTES, dtype=np.int8)

    column_description.max_length = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    # Need to read the table record for the column
    column_description.keywords = read_record(file_handle)

    # Random read?
    file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    # Not sure where this is used, I don't see a use for the _default parameter in the original code.
    # For that reason, I am going to just read the value but not save it for anything till something breaks.
    if "ArrayColumnDesc" in column_description.type:
        file_handle.read(types.ONE_BYTE)

    elif column_description.value_type in types.TYPE_TO_BYTES.keys():
        file_handle.read(types.TYPE_TO_BYTES[column_description.value_type])

    elif column_description.value_type == "string":
        file_handle.string(size=types.FOUR_BYTES)

    else:
        logger.error(
            f"Can't fine associated read for column type: {column_description.type} or {column_description.value_type}")
        raise NotImplementedError()

    return column_description


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
