import numpy as np

from toolviper.utils import logger
from dataclasses import dataclass

from typing import Callable

from mio.core.binary import BinaryFileReader
from mio.utilities import types


@dataclass(init=False)
class Block:
    nrows: int
    name: str
    version: int
    size: int
    elements: list


def read_block(file_handle: BinaryFileReader, function: Callable) -> Block:
    block = Block()

    # Version(1) header
    block.nrows = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    block.name = file_handle.string(size=types.FOUR_BYTES)

    block.version = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    # Block
    block.size = file_handle.integer(size=types.FOUR_BYTES, dtype=np.int32)

    block.elements = [function(size=types.FOUR_BYTES, dtype=np.int32) for _ in range(block.size)]

    return block

