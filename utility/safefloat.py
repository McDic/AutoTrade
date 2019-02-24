"""
<module utility.safefloat>
This module is used to calculate arithmetic decimal operations under automated quantize process.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
import decimal

# ----------------------------------------------------------------------------------------------------------------------
# SafeFloat: Safe float for decimal

class SafeFloat:
    """
    <class SafeFloat>
    Represents decimal number with fixed under(negative) digits under given condition.
    """

    # ------------------------------------------------------------------------------------------------------------------
    # Constructor

    def __init__(self, num, maxUnderDigits: int = 8):
        self.maxUnderDigits = maxUnderDigits
        if isinstance(num, (int, float, str, decimal.Decimal)): self.num = decimal.Decimal(num)
        elif isinstance(num, SafeFloat): self.num = num.num
        else: raise TypeError("Invalid type %s for num given" % (type(num),))
        self.num = self.num.quantize(decimal.Decimal("0.1") ** maxUnderDigits)

    # ------------------------------------------------------------------------------------------------------------------
    # Representation

    def __str__(self): return str(self.num)
    def __repr__(self): return "SafeFloat(%s, under %d digits)" % (str(self.num), self.maxUnderDigits)

    # ------------------------------------------------------------------------------------------------------------------
    # Arithmetic operation

    def digitResult(self, other): # Return integrated digit
        return max(self.maxUnderDigits, other.maxUnderDigits) if isinstance(other, SafeFloat) else self.maxUnderDigits

    def __neg__(self): # - self
        return SafeFloat(-self.num, maxUnderDigits = self.maxUnderDigits)

    def __add__(self, other): # self + other
        return SafeFloat(self.num + other, maxUnderDigits = self.digitResult(other))
    __radd__ = __add__ # other + self

    def __sub__(self, other): # self - other
        return SafeFloat(self.num - other, maxUnderDigits = self.digitResult(other))
    def __rsub__(self, other): # other - self
        return SafeFloat(other - self.num, maxUnderDigits = self.digitResult(other))

    def __mul__(self, other): # self * other
        return SafeFloat(self.num * other, maxUnderDigits = self.digitResult(other))
    __rmul__ = __mul__ # other * self

    def __truediv__(self, other): # self / other
        return SafeFloat(self.num / other, maxUnderDigits = self.digitResult(other))
    def __rtruediv__(self, other): # other / self
        return SafeFloat(other / self.num, maxUnderDigits = self.digitResult(other))

    def __floordiv__(self, other): # self // other
        return SafeFloat(self.num // other, maxUnderDigits = self.digitResult(other))
    def __rfloordiv__(self, other): # other // self
        return SafeFloat(other // self.num, maxUnderDigits = self.digitResult(other))

    def __pow__(self, power): # self ** power
        return SafeFloat(self.num ** power, maxUnderDigits = self.maxUnderDigits)
    def __rpow__(self, other): # other ** self
        return SafeFloat(other ** self.num, maxUnderDigits = other.maxUnderDigits if isinstance(other, SafeFloat) else self.maxUnderDigits)

    def __int__(self): return int(self.num) # int(self)
    def __float__(self): return float(self.num) # float(self)
    def __abs__(self): return abs(self.num) # abs(self)
    def __bool__(self): return bool(self.num) # bool(self)
    def __complex__(self): return complex(float(self), 0) # complex(self)

# ----------------------------------------------------------------------------------------------------------------------
# Functionality testing

if __name__ == "__main__":
    sf = SafeFloat("1.234")
    print(sf + 1)