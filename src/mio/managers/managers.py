import numpy as np

from mio.utilities import types

from mio.core.binary import BinaryFileReader
from mio.core.block import read_block, Block

from toolviper.utils import logger


class StandardStorageManager:
    __slots__ = ["name", "offset", "index_map", "file_handle"]

    def __init__(self):
        self.name = None
        self.offset = None
        self.index_map = None
        self.file_handle = None

    @classmethod
    def read(cls, file_handle: BinaryFileReader):
        file_handle.header()
        cls.name = file_handle.string(size=types.FOUR_BYTES)

        cls.offset = read_block(
            file_handle,
            file_handle.integer
        )

        cls.index_map = read_block(
            file_handle,
            file_handle.integer
        )

        cls.file_handle = file_handle

        return cls


class IncrementalStorageManager:
    __slots__ = ["name"]

    def __init__(self):
        self.name = None

    @classmethod
    def read(cls, file_handle):
        file_handle.header()
        cls.name = file_handle.string(size=types.FOUR_BYTES)

        return cls


class TiledCellStorageManager:
    pass


class TiledShapeStorageManager:
    __slots__ = ["name"]

    def __init__(self):
        self.name = None

    @classmethod
    def read(cls, file_handle):
        return cls


class TiledColumnStorageManager:
    __slots__ = ["name"]

    def __init__(self):
        self.name = "TiledColumnStMan"

    @classmethod
    def read(cls, file_handle):
        return cls


class AipsIOStorageManager:
    pass
