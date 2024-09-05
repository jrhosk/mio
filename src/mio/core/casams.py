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
        self.handler.check_type()

        # Read important meta data
        self.nrows = self.handler.integer(size=types.FOUR_BYTES, dtype=np.int32)
        self.format = self.handler.integer(size=types.FOUR_BYTES, dtype=np.int32)
        self.name = self.handler.string(size=types.FOUR_BYTES)

        logger.debug(f"|File information>: name({self.name}), format({self.format}) nrows({self.nrows})")

        # Read table description
        # Get type name and version number (again)
        self.handler.check_type()

        # Read unknown strings
        logger.debug("Reading unknown strings ...")
        for _ in range(3):
            logger.debug(self.handler.string(size=types.FOUR_BYTES))

        # Table description struct
        self.table = TableDescription()

        self.table.keywords = table.read_record(self.handler)
        for key, value in self.table.keywords.records.items():
            logger.debug(f"|key>: {key}, \t|value> {value}")

        self.table.private = table.read_record(self.handler)
        for key, value in self.table.private.records.items():
            logger.debug(f"|key>: {key}, \t|value> {value}")







