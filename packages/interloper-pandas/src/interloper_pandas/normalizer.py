import datetime as dt
from dataclasses import dataclass, field
from typing import Any

import interloper as itlp
import pandas as pd


@dataclass
class DataframeNormalizer(itlp.Normalizer):
    separator: str = "_"
    max_level: int = 0
    rename_columns: bool = field(default=True, kw_only=True)
    remove_empty_dict: bool = field(default=True, kw_only=True)
    remove_empty_strings: bool = field(default=True, kw_only=True)
    drop_na_columns: bool = field(default=True, kw_only=True)
    drop_empty_lists_columns: bool = field(default=True, kw_only=True)
    convert_na_columns_to_string: bool = field(default=False, kw_only=True)
    convert_object_columns_to_string: bool = field(default=False, kw_only=True)

    def normalize(self, data: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(data, pd.DataFrame):
            raise itlp.AssetNormalizationError(
                f"DataframeNormalizer requires pandas DataFrame, got {type(data).__name__}"
            )

        # TODO: optimize this: check only columns of type dict and normalize them
        if self.max_level > 0:
            data = pd.json_normalize(
                data.to_dict(orient="records"),
                sep=self.separator,
                max_level=self.max_level,
            )

        if self.rename_columns:
            data.columns = [self.column_name(column) for column in data.columns]

        if self.remove_empty_dict:
            data.replace([{}], None, inplace=True)

        if self.remove_empty_strings:
            data.replace([""], None, inplace=True)

        if self.drop_na_columns:
            data.dropna(how="all", axis=1, inplace=True)

        if self.drop_empty_lists_columns:
            empty_list_columns = [
                col for col in data.columns if all(isinstance(value, list) and not value for value in data[col])
            ]
            data.drop(empty_list_columns, axis=1, inplace=True)

        if self.convert_na_columns_to_string:
            null_columns = data.columns[data.isna().all()]
            data[null_columns] = data[null_columns].astype("string")

        # TODO: review this: should probably target `dict` columns specifically
        if self.convert_object_columns_to_string:
            object_columns = data.select_dtypes(include="object").columns
            data[object_columns] = data[object_columns].astype("string")

        return data

    def infer_schema(self, data: pd.DataFrame) -> type[itlp.TableSchema]:
        schema_dict = {}
        for column in data.columns:
            dtype = data[column].dtype
            schema_dict[column] = self._dtype_to_python_type(dtype)
        return itlp.TableSchema.from_dict(schema_dict, name="Inferred")

    def _dtype_to_python_type(self, dtype: Any) -> type:
        if pd.api.types.is_integer_dtype(dtype):
            return int
        elif pd.api.types.is_float_dtype(dtype):
            return float
        elif pd.api.types.is_bool_dtype(dtype):
            return bool
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return dt.datetime
        elif pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            return str
        else:
            return str
