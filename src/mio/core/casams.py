import pathlib

import numpy as np
import graphviper.utils.logger as logger

from mio.core import binary
from mio.core import table
from mio.utilities import types

from dataclasses import dataclass


@dataclass(init=False)
class TableDescription:
    ncolumns: int
    column_description: list
    keywords: table.TableRecord()
    private: table.TableRecord()


class CasaMeasurementSet:
    __slots__ = ["filename", "handler", "nrows", "format", "name", "description", "column_set", "table"]

    def __init__(self, filename):
        self.filename = filename
        self.handler = binary.BinaryFileReader(filename, mode="rb")

        self.nrows = None
        self.format = None
        self.name = None
        self.description = None
        self.column_set = None
        self.table = None

    def read(self):
        # Get type name and version number
        self.handler.header()

        # Read important meta data
        self.nrows = self.handler.integer(size=types.FOUR_BYTES, dtype=np.int32)
        self.format = self.handler.integer(size=types.FOUR_BYTES, dtype=np.int32)
        self.name = self.handler.string(size=types.FOUR_BYTES)

        # Read table description
        # Get type name and version number (again)
        self.handler.header()

        # Read unknown strings
        for _ in range(3):
            self.handler.string(size=types.FOUR_BYTES)

        # Table description struct
        self.table = TableDescription()

        # Read table keywords
        self.table.keywords = table.read_record(self.handler)

        # Read private keywords
        self.table.private = table.read_record(self.handler)

        # Get number of columns
        self.table.ncolumns = self.handler.integer(size=types.FOUR_BYTES, dtype=np.int32)

        # Get list of columns
        self.description = [table.read_column_description(self.handler) for _ in range(self.table.ncolumns)]

        self.column_set = table.read_column_set(self.handler, self.description)

        for index, manager in self.column_set.data_managers.items():
            logger.info(f"{manager}: {pathlib.Path(self.filename).resolve().joinpath(f'table.f{index}')}")







