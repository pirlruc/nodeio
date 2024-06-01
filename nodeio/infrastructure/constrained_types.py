from pydantic import StringConstraints
from typing import Annotated

key_str = Annotated[str,StringConstraints(strip_whitespace=True,min_length=1)]
