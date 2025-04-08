import logging
from collections.abc import Callable
from typing import NamedTuple, TypeVar

import interloper as itlp
import numpy as np
import pandas as pd

T = TypeVar("T")
logger = logging.getLogger(__name__)


class _SQLToDTypeMap(NamedTuple):
    type: str
    convert_fn: Callable[[pd.Series], pd.Series] | None = None


SQL_TYPE_TO_DTYPE = {
    "STRING": _SQLToDTypeMap(type="string"),
    "VARCHAR": _SQLToDTypeMap(type="string"),
    "BOOLEAN": _SQLToDTypeMap(type="bool"),
    "INTEGER": _SQLToDTypeMap(type="int64"),
    "FLOAT": _SQLToDTypeMap(type="float64"),
    "DOUBLE": _SQLToDTypeMap(type="float64"),
    "NUMERIC": _SQLToDTypeMap(type="float64"),
    "DATE": _SQLToDTypeMap(
        type="datetime64[s]",
        convert_fn=lambda s: pd.to_datetime(s, utc=True).dt.tz_localize(None).dt.date,
    ),
    "DATETIME": _SQLToDTypeMap(
        type="datetime64[s]",
        convert_fn=lambda s: pd.to_datetime(s, utc=True).dt.tz_localize(None),
    ),
    "TIMESTAMP": _SQLToDTypeMap(
        type="datetime64[s]",
        convert_fn=lambda s: pd.to_datetime(s, utc=True).dt.tz_localize(None),
    ),
}


class DataFrameReconciler(itlp.Reconciler[pd.DataFrame]):
    def reconcile(
        self,
        data: pd.DataFrame,
        table_schema: dict[str, str],
        reorder: bool = False,
    ) -> pd.DataFrame:
        logger.info("Reconciling dataframe with SQL schema...")
        data = data.copy()

        if set(data.columns) != set(table_schema.keys()):
            logger.warning("Columns do not match the provided schema.")

        extra_data_columns = set(data.columns) - set(table_schema.keys())
        data.drop(columns=list(extra_data_columns), inplace=True)

        for index, (column, sql_type) in enumerate(table_schema.items()):
            if column not in data.columns:
                logger.debug(f"Column {column} is missing from dataframe. Adding empty column...")
                data.insert(index, column, np.nan)

            if sql_type in SQL_TYPE_TO_DTYPE:
                current_dtype = data[column].dtype
                target_dtype, convert_fn = SQL_TYPE_TO_DTYPE[sql_type]
                logger.debug(f"Converting column '{column}' from {current_dtype} to {target_dtype} (Schema {sql_type})")
                try:
                    if convert_fn:
                        data[column] = convert_fn(data[column])  # type: ignore
                    else:
                        data[column] = data[column].astype(target_dtype)  # type: ignore
                except Exception as e:
                    logger.error(f"Failed to convert column '{column}' to {target_dtype}: {e}")

            else:
                logger.warning(f"Unsupported SQL type {sql_type} for column {column}. Unable to reconcile.")

        if reorder:
            logger.info("Reordering columns to match schema...")
            data = data[table_schema.keys()]  # type: ignore

        logger.info("Schema reconciliation complete.")
        return data
