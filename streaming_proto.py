import abc
import re

class FieldValue(abc.ABC):

    def __init__(self, field):
        assert isinstance(field, int) and field >= 0
        self.field = field

    @abc.abstractmethod
    def encode(self):
        """Returns the header byte sequence and the payload for self,
        and if the payload is a chunk terminator.
        """

def toMinRadix253(value):
    assert isinstance(value, int) and value >= 0
    if value == 0:
        return bytes([0])

    limbs = []
    while value > 0:
        limbs.append(value % 253)
        value //= 253
    return bytes(limbs)

class IntegerField(FieldValue):
    def __init__(self, field, value):
        super().__init__(field)
        assert isinstance(value, int) and value >= -(2**63) and value < 2**64
        self.value = value

    @staticmethod
    def metadata(field, *, width, extraBit):
        assert width in (1, 2, 4, 8)
        widthIdx = width.bit_count() - 1

        topBits = (widthIdx << 5) | (extraBit << 7)
        if 0 <= field < 16:
            return bytes([field | topBits])

        fieldLimbs = toMinRadix253(field)
        assert 1 <= len(fieldLimbs) <= 4
        header = (len(fieldLimbs) - 1) | 16 | topBits
        return bytes([header]) + fieldLimbs

    @staticmethod
    def toRadix253(value, *, width):
        limbs = []  # Little endian

        toEncode = value
        if (toEncode < 0):
            toEncode = (~toEncode) + 256**width
        for _ in range(width):
            limbs.append(toEncode % 253)
            toEncode //= 253

        assert 0 <= toEncode <= 1, f"value too large. value={value} width={width}"
        return bytes(limbs), toEncode

    def encode(self):
        if self.value >= 0 and self.value < 253:
            payload, extraBit = IntegerField.toRadix253(self.value, width=1)
            assert extraBit == 0
            return (IntegerField.metadata(self.field, width=1, extraBit=extraBit),
                    payload,
                    False)

        for width in (2, 4, 8):
            if self.value >= -(2 ** (8 * width - 1)) and self.value < 2**(8 * width):
                payload, extraBit = IntegerField.toRadix253(self.value, width=width)
                return (IntegerField.metadata(self.field, width=width, extraBit=extraBit),
                        payload,
                        False)

        raise RuntimeError(f"value out of range {self.value}")


class RawField(FieldValue):
    def __init__(self, field, value):
        super().__init__(field)
        assert isinstance(value, bytes) and 1 <= len(value) <= 22 and RawPayload.isSafe(value)
        self.value = value

    @staticmethod
    def isSafe(value):
        if value.find(b"\xfe\xfd") >= 0:
            return False
        if value[0] == b"\xfd":
            return False

    def encode(self):
        return (RawField.metadata(self.field, len(self.value)), self.value, False)

    
class GenericField(FieldValue):
    def __init__(self, field, value):
        super().__init__(field)
        self.value = byteStuff(value)

    #def encode(self):
