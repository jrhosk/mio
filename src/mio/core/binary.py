import struct
import numpy as np

import graphviper.utils.logger as logger

from io import FileIO

from mio.utilities import types


class BinaryFileReader(FileIO):
    logger.get_logger().setLevel('DEBUG')

    def __init__(self, file, mode):
        super().__init__(file, mode)
        self.filename = file
        #self.values = Vector()
        self.endian = "<"

        self._check_magic()
        self._check_endianess()

    def _check_magic(self):
        if self.read(types.FOUR_BYTES) != b"\xbe\xbe\xbe\xbe":
            logger.error("Bad magic code.")
            self.close()

        logger.debug("Reading magic code ...")

    def _check_endianess(self):
        if self.read(types.ONE_BYTE) == b"\x00":
            self.endian = ">"
            self.seek(types.FOUR_BYTES)

        logger.debug(f"Reading endianess ... {self.endian}")

    def check_type(self):
        _ = self.integer(size=types.FOUR_BYTES, dtype=np.int32)
        table_type = self.string(size=types.FOUR_BYTES)
        version = self.integer(size=types.FOUR_BYTES, dtype=np.int32)

        return table_type, version

    def string(self, size):
        length = self.integer(size=size, dtype=np.int32)
        data = self.read(length)

        try:
            return data.replace(b"\x00", b"").decode("ascii")

        except UnicodeEncodeError as error:
            print("Couldn't decode data due to following error. Returning raw data.")
            print(error)

            return data

    def boolean(self):
        return self.read(types.ONE_BYTE) == b"\0x01"

    def integer(self, size, dtype):
        """
        :param size: int
            Buffer size in bytes to read.
        :param dtype: int
            data type to convert buffer to.
        :return: int
            Converted data buffer.
        """
        return dtype(struct.unpack(f"{self.endian}{types.BUFFER_FORMAT[dtype.__name__]}", self.read(size))[0])

    def float(self, size, dtype):
        return dtype(struct.unpack(f"{self.endian}{types.BUFFER_FORMAT[dtype.__name__]}", self.read(size))[0])

    def complex(self, size, dtype):
        csize = types.EIGHT_BYTES if size == types.SIXTEEN_BYTES else types.FOUR_BYTES
        ctype = np.float64 if size == types.SIXTEEN_BYTES else np.float32

        return dtype(self.float(size=csize, dtype=ctype) + ij * self.float(size=csize, dtype=ctype))

    def array(self, atype):
        self.check_type()

        ndim = self.integer(size=types.FOUR_BYTES, dtype=np.int32)
        shape = [self.integer(size=types.FOUR_BYTES, dtype=np.int32) for _ in range(ndim)]
        size = self.integer(size=types.FOUR_BYTES, dtype=np.int32)

        if atype == 'string':
            array = np.array([self.string(size=types.FOUR_BYTES) for i in range(size)])

        elif atype == 'bool':
            length = int(np.ceil(size / 8)) * 8
            array = np.unpackbits(
                np.frombuffer(self.read(length), dtype='uint8'), bitorder='little'
            ).astype(bool)[:size]

        elif atype in types.DATA_TYPE:
            dtype = np.dtype(self.endian + types.DATA_TYPE[atype])
            array = np.frombuffer(self.read(int(size * dtype.itemsize)), dtype=dtype)

        else:
            raise NotImplementedError(f"Can't read in data of type {atype}")

        if shape is not None:
            array = array.reshape(shape)

        return array

    def position(self, size, dtype):
        table_type, version = self.check_type()
        length = self.integer(size=size, dtype=dtype)

        return np.array([self.integer(size=size, dtype=dtype) for _ in range(length)], dtype=int)
