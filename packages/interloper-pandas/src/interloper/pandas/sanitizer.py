from typing import Any, TypeVar

import pandas as pd
from interloper.core.sanitizer import Sanitizer

T = TypeVar("T")


class DataFrameSanitizer(Sanitizer[pd.DataFrame]):
    def sanitize(self, data: pd.DataFrame, inplace: bool = False) -> Any:
        if not inplace:
            data = data.copy()

        data.columns = [self.column_name(col) for col in data.columns]
        return data
