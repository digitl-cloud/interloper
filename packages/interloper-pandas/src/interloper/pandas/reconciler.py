import logging
from collections.abc import Callable
from typing import NamedTuple, TypeVar

import numpy as np

import pandas as pd
from interloper.core.reconciler import Reconciler
from pandas._typing import AstypeArg
from pandas.api import types

T = TypeVar("T")
logger = logging.getLogger(__name__)


class _SQLToDTypeMap(NamedTuple):
    type: str
    check_fn: Callable[[AstypeArg], bool]
    convert_fn: Callable[[pd.Series], pd.Series] | None = None


SQL_TYPE_TO_DTYPE = {
    "STRING": _SQLToDTypeMap(
        type="string",
        check_fn=types.is_string_dtype,
    ),
    "VARCHAR": _SQLToDTypeMap(
        type="string",
        check_fn=types.is_string_dtype,
    ),
    "BOOLEAN": _SQLToDTypeMap(
        type="bool",
        check_fn=types.is_bool_dtype,
    ),
    "INTEGER": _SQLToDTypeMap(
        type="Int64",
        check_fn=types.is_integer_dtype,
    ),
    "FLOAT": _SQLToDTypeMap(
        type="Float64",
        check_fn=types.is_float_dtype,
    ),
    "DOUBLE": _SQLToDTypeMap(
        type="Float64",
        check_fn=types.is_float_dtype,
    ),
    "NUMERIC": _SQLToDTypeMap(
        type="Float64",
        check_fn=types.is_float_dtype,
    ),
    "DATE": _SQLToDTypeMap(
        type="datetime64[s]",
        check_fn=types.is_datetime64_any_dtype,
        convert_fn=lambda s: pd.to_datetime(s, utc=True).dt.tz_localize(None).dt.date,
    ),
    "DATETIME": _SQLToDTypeMap(
        type="datetime64[s]",
        check_fn=types.is_datetime64_any_dtype,
        convert_fn=lambda s: pd.to_datetime(s, utc=True).dt.tz_localize(None),
    ),
    "TIMESTAMP": _SQLToDTypeMap(
        type="datetime64[s]",
        check_fn=types.is_datetime64_any_dtype,
        convert_fn=lambda s: pd.to_datetime(s, utc=True).dt.tz_localize(None),
    ),
}


class DataFrameReconciler(Reconciler[pd.DataFrame]):
    def reconcile(
        self,
        data: pd.DataFrame,
        schema: dict[str, str],
        reorder: bool = False,
        force: bool = False,
    ) -> pd.DataFrame:
        logger.info("Reconciling dataframe with SQL schema...")
        data = data.copy()

        if set(data.columns) != set(schema.keys()):
            logger.warning("Columns do not match the provided schema.")

        extra_data_columns = set(data.columns) - set(schema.keys())
        data.drop(columns=list(extra_data_columns), inplace=True)

        for index, (column, sql_type) in enumerate(schema.items()):
            if column not in data.columns:
                logger.debug(f"Column {column} is missing from dataframe. Adding empty column...")
                data.insert(index, column, np.nan)

            if sql_type in SQL_TYPE_TO_DTYPE:
                current_dtype = data[column].dtype
                target_dtype, check_fn, convert_fn = SQL_TYPE_TO_DTYPE[sql_type]

                if force or not check_fn(current_dtype):
                    logger.debug(
                        f"Converting column '{column}' from {current_dtype} to {target_dtype} (Schema: {sql_type})"
                        + ("(Forced)" if force else "")
                    )

                    try:
                        if convert_fn:
                            data[column] = convert_fn(data[column])
                        else:
                            data[column] = data[column].astype(target_dtype)  # type: ignore # TODO: fix typing
                    except Exception as e:
                        logger.error(f"Failed to convert column '{column}' to {target_dtype}: {e}")

            else:
                logger.warning(f"Unsupported SQL type {sql_type} for column {column}. Unable to reconcile.")

        if reorder:
            # TODO: check this is working
            logger.info("Reordering columns to match schema...")
            data = data[schema.keys()]

        logger.info("Schema reconciliation complete.")
        return data
