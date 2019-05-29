"""
<module simuator.errors>
Exceptions for simulator module.
"""

# ----------------------------------------------------------------------------------------------------------------------
# Libraries

# Standard libraries
from decimal import Decimal

# ----------------------------------------------------------------------------------------------------------------------
# Exceptions

class AbstractSimulatorError(Exception):
    """
    <class AbstractSimulatorError> inherited from Exception
    Abstract base of all simulator errors.
    """

class AbstractFakeAccountError(AbstractSimulatorError):
    """
    <class AbstractFakeAccountError>
    """

class PastAccessError(AbstractSimulatorError):
    """
    <class PastAccessError> inherited from AbstractSimulatorError
    Raised when you tried to access past time(before fake account's current time).
    """

class NotEnoughBalance(AbstractFakeAccountError):
    """
    <class NotEnoughBalance> inherited from AbstractFakeAccountError
    Raised when you tried to trade more than your available balance.
    """
    def __init__(self, currency: str, triedAmount: (float, Decimal), actualBalance: (float, Decimal)):
        super().__init__("Tried to remove %s %s while having %s %s" % (triedAmount, currency, actualBalance, currency))