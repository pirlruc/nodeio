"""The constrained types module implements custom pydantic data fields.

Data Types:
    KeyStr - string data type that should have at least one character
     different than a space.
"""

from typing import Annotated

from pydantic import StringConstraints

KeyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
