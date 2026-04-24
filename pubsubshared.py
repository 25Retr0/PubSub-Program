"""! @file pubsubclient.py
@author William Kelly (s4882158)
@ai Not Used
"""

from typing import Optional

### Functions ##################################################################

def isValidId(id: str) -> bool:
    """Given an id, returns True if:
        - must be between 2 and 32 characters (inclusive) in length.
        - contain only letters and/or digits
    """
    return ((2 <= len(id) <=32) and id.isalnum());
