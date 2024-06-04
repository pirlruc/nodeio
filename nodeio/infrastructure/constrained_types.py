from typing import Annotated

from pydantic import StringConstraints

key_str = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1)
]
