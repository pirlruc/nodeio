from typing import Annotated

from pydantic import StringConstraints

KeyStr = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1)
]
