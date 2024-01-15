from collections import defaultdict
from enum import IntEnum
import struct

def radix_decode(header, *, radix, count=None):
    """Convers `header` bytes from little-endian with `radix` to
    non-negative integer.  If `count` is provided, decodes exactly
    `count` bytes, otherwise, reads everything in `header`.
    """
    count = len(header) if count is None else count
    ret = 0
    scale = 1
    for idx in range(count):
        ret += header[idx] * scale
        scale *= radix
    return ret


def radix_encode(value, *, radix):
    """Encodes `value` to little endian bytes with `radix`."""
    assert isinstance(value, int) and value >= 0
    assert isinstance(radix, int) and 0 < radix <= 256
    ret = []
    while value > 0 or not ret:
        ret.append(struct.pack("<B", value % radix))
        value //= radix
    return b"".join(ret)


def _chunk_frame(data):
    """Returns the list of chunk headers and chunk data to frame `data`"""
    chunks = []
    max_chunk_size = 252
    while data:
        first_chunk = not chunks
        max_chunk_size = 252 if first_chunk else (253 * 253) - 1
        current_chunk_size = min(max_chunk_size, len(data))
        chunk_data = data[:current_chunk_size]

        if first_chunk:
            assert max_chunk_size == 252
            chunk_header = struct.pack("<B", current_chunk_size)
        else:
            assert max_chunk_size == (253 * 253) - 1
            chunk_header = struct.pack("<BB", current_chunk_size % 253, current_chunk_size // 253)

        assert current_chunk_size == len(chunk_data) == radix_decode(chunk_header, radix=253)
        chunks.append(chunk_header)
        chunks.append(chunk_data)
        data = data[current_chunk_size:]

    return chunks


def chunk_frame(data):
    """Converts a frame of `data` bytes to size-prefixed chunks."""
    data = memoryview(data)
    assert len(data) > 0, "Frame must not be empty"
    return b"".join(_chunk_frame(data))


class IndexMetadataType(IntEnum):
    IDX_1 = 0
    IDX_2 = 1
    IDX_3 = 2
    IDX_4 = 3
    IDX_5 = 4
    IDX_6 = 5
    IDX_7 = 6
    IDX_8 = 7
    IDX_9 = 8
    IDX_10 = 9
    IDX_11 = 10
    IDX_12 = 11
    IDX_13 = 12
    IDX_14 = 13
    IDX_15 = 14
    OUT_OF_LINE_1 = 15  # One radix-128 byte
    OUT_OF_LINE_2 = 16  # Two radix-128 bytes
    OUT_OF_LINE_3 = 17
    OUT_OF_LINE_4 = 18
    OUT_OF_LINE_5 = 19  # Five radix-128 bytes
    SENTINEL = 20

OUT_OF_LINE_INDEXES = {
    1: IndexMetadataType.OUT_OF_LINE_1,
    2: IndexMetadataType.OUT_OF_LINE_2,
    3: IndexMetadataType.OUT_OF_LINE_3,
    4: IndexMetadataType.OUT_OF_LINE_4,
    5: IndexMetadataType.OUT_OF_LINE_5,
}


OUT_OF_LINE_BYTES_FOR_METADATA = defaultdict(int)
OUT_OF_LINE_BYTES_FOR_METADATA.update((v, k) for k, v in OUT_OF_LINE_INDEXES.items())


class ValueMetadataType(IntEnum):
    IMPLICIT_BYTESTRING = 0
    EXPLICIT_BYTESTRING = 1
    ATOMIC_1 = 2  # 1-byte atomic value
    ATOMIC_2 = 3  # 2-byte atomic value
    ATOMIC_4 = 4  # 4-byte atomic value
    ATOMIC_8 = 5  # 5-byte atomic value

ATOMIC_VALUE_TYPES = {
    1: ValueMetadataType.ATOMIC_1,
    2: ValueMetadataType.ATOMIC_2,
    4: ValueMetadataType.ATOMIC_4,
    8: ValueMetadataType.ATOMIC_8,
}

EXPLICIT_SIZE_BYTES_FOR_METADATA = defaultdict(int)
EXPLICIT_SIZE_BYTES_FOR_METADATA.update({ValueMetadataType.EXPLICIT_BYTESTRING: 1})


def encode_combined_metadata(index_metadata, value_metadata):
    assert isinstance(value_metadata, ValueMetadataType)
    assert isinstance(index_metadata, IndexMetadataType)
    assert value_metadata != ValueMetadataType.ATOMIC_8 or index_metadata != IndexMetadataType.SENTINEL
    ret = 128 + int(value_metadata) * 21 + int(index_metadata)
    assert 128 <= ret < 253
    assert bool(value_metadata == ValueMetadataType.IMPLICIT_BYTESTRING) == bool(128 <= ret <= 148), \
        f"{value_metadata} {ret} {value_metadata == ValueMetadataType.IMPLICIT_BYTESTRING} == {128 <= ret < 148}"
    return struct.pack("<B", ret)


def end_of_metadata_sentinel_byte():
    ret = encode_combined_metadata(IndexMetadataType.SENTINEL, ValueMetadataType.IMPLICIT_BYTESTRING)
    assert ret == bytes([148])
    return ret


def _metadata_header_bytes(field_index, value_size, atomic_value):
    assert isinstance(field_index, int) and 0 < field_index < 2**35
    assert value_size is None or (isinstance(value_size, int) and 0 <= value_size < 128)

    if field_index < 16:
        field_index_prefix = b""
        index_type = IndexMetadataType(field_index - 1)
    else:
        field_index_prefix = radix_encode(field_index, radix=128)
        assert field_index == radix_decode(field_index_prefix, radix=128)
        index_type = OUT_OF_LINE_INDEXES[len(field_index_prefix)]

    if atomic_value:
        assert value_size in (1, 2, 4, 8)
        value_type = ATOMIC_VALUE_TYPES[value_size]
        size_suffix = b""
    elif value_size is not None:
        assert 0 <= value_size < 128
        value_type = ValueMetadataType.EXPLICIT_BYTESTRING
        size_suffix = radix_encode(value_size, radix=128)
        assert value_size == radix_decode(size_suffix, radix=128, count=1)
        assert len(size_suffix) == 1
    else:
        value_type = ValueMetadataType.IMPLICIT_BYTESTRING
        size_suffix = b""
    return field_index_prefix, index_type, value_type, size_suffix


def metadata_header_bytes(field_index, value_size, atomic_value):
    field_index_prefix, index_type, value_type, size_suffix = _metadata_header_bytes(field_index, value_size, atomic_value)
    assert len(field_index_prefix) == OUT_OF_LINE_BYTES_FOR_METADATA[index_type]
    assert all(0 <= digit < 128 for digit in field_index_prefix)
    assert all(0 <= digit < 128 for digit in size_suffix)
    assert len(size_suffix) == EXPLICIT_SIZE_BYTES_FOR_METADATA[value_type]
    ret = field_index_prefix + encode_combined_metadata(index_type, value_type) + size_suffix
    # header bytes must be < 253
    assert all(0 <= digit < 253 for digit in ret)
    # Exactly one byte is >= 128 (the combined metadata byte)
    assert sum(digit >= 128 for digit in ret) == 1
    # That combined byte is the last or second to last byte
    assert ret[-1] >= 128 or ret[-2] >= 128
    return ret


class FrameBuilder:

    def __init__(self):
        self.metadata = []
        self.data = []
        self.terminated = False  # True when the frame is terminated with a sentinel or an implicitly sized bytestring

    def _raw_bytes(self):
        assert sum(len(header) for header in self.metadata) <= 251
        return b"".join(self.metadata + self.data)

    def _encode(self):
        return chunk_frame(self._raw_bytes())

    def encode(self):
        assert self.terminated
        return self._encode()

    def terminate(self):
        assert not self.terminated
        self.metadata.append(end_of_metadata_sentinel_byte())
        self.terminated = True
        assert sum(len(header) for header in self.metadata) <= 251
        return self

    def atomic(self, field_index, data):
        assert not self.terminated
        assert 0 < field_index < 2**35
        assert len(data) in (1, 2, 4, 8)
        self.metadata.append(metadata_header_bytes(field_index, len(data), atomic_value=True))
        self.data.append(data)
        assert sum(len(header) for header in self.metadata) <= 251
        return self

    def bytestring(self, field_index, data, *, final=False):
        assert not self.terminated
        assert 0 < field_index < 2**35
        assert len(data) < 128 or final
        if final:
            self.terminated = True
            self.metadata.append(metadata_header_bytes(field_index, None, atomic_value=False))
            self.data.append(data)
        else:
            self.metadata.append(metadata_header_bytes(field_index, len(data), atomic_value=False))
            self.data.append(data)
        assert sum(len(header) for header in self.metadata) <= 251
        return self

    _BITS_TO_UPACK = {
         8: "<B",
        16: "<H",
        32: "<I",
        64: "<Q",
    }

    def uint(self, field_index, value, *, bits=None):
        assert isinstance(value, int) and 0 <= value < 2**64
        for min_bits in (8, 16, 32, 64):
            if 0 <= value < 2**min_bits:
                break

        wanted_bits = bits if bits is not None else min_bits
        self.atomic(field_index, struct.pack(FrameBuilder._BITS_TO_UPACK[wanted_bits], value))
        return self

    _BITS_TO_SPACK = {
         8: "<b",
        16: "<h",
        32: "<i",
        64: "<q",
    }

    def sint(self, field_index, value, *, bits=None):
        assert isinstance(value, int) and -2**63 <= value < 2**63
        for min_bits in (8, 16, 32, 64):
            if -2**(min_bits - 1) <= value < 2**(min_bits - 1):
                break

        wanted_bits = bits if bits is not None else min_bits
        self.atomic(field_index, struct.pack(FrameBuilder._BITS_TO_SPACK[wanted_bits], value))
        return self
