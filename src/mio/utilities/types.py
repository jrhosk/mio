ONE_BYTE: int = 1
TWO_BYTES: int = 2
FOUR_BYTES: int = 4
EIGHT_BYTES: int = 8
SIXTEEN_BYTES: int = 16


DATA_TYPE = {
    "dcomplex": "c16",
    "complex": "c8",
    "double": "f8",
    "float": "f4",
    "int": "i4",
    "uint": "u4",
    "short": "i2",
    "string": "U",
    "bool": "bool",
    "record": "O"
}

BUFFER_FORMAT = {
    "int8": "i",
    "int16": "i",
    "int32": "i",
    "int64": "q",
    "float32": "f",
    "float64": "d"
}

TYPE_LIST = [
    'bool',
    'char',
    'uchar',
    'short',
    'ushort',
    'int',
    'uint',
    'float',
    'double',
    'complex',
    'dcomplex',
    'string',
    'table',
    'arraybool',
    'arraychar',
    'arrayuchar',
    'arrayshort',
    'arrayushort',
    'arrayint',
    'arrayuint',
    'arrayfloat',
    'arraydouble',
    'arraycomplex',
    'arraydcomplex',
    'arraystr',
    'record',
    'other'
]