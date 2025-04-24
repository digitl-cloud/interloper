import datetime as dt
from inspect import currentframe, signature
from unittest.mock import patch

import interloper as itlp
import pandas as pd
import pytest
from google.cloud import bigquery
from interloper_google_cloud import BigQueryIO
from interloper_pandas import DataframeNormalizer
from pandas.testing import assert_frame_equal

BIGQUERY_DATASET = "interloper_functional_tests"


def fetch_data(client: bigquery.Client, table: str) -> pd.DataFrame:
    query = f"SELECT * FROM {table}"
    return client.query(query).to_dataframe()


@pytest.fixture(scope="session")
def client() -> bigquery.Client:
    client = bigquery.Client(project="dc-int-connectors-prd")
    client.delete_dataset(BIGQUERY_DATASET, delete_contents=True, not_found_ok=True)
    yield client
    client.delete_dataset(BIGQUERY_DATASET, delete_contents=True, not_found_ok=True)


@pytest.fixture
def source() -> itlp.Source:
    """
    This fixture is used to create a source with an asset.
    The data function is patched, which allows to easily customize on the fly the data returned by the asset.

    Example:
    ```py
    def test_bigquery_flexible(source: itlp.Source):
        source.asset.data.return_value = pd.DataFrame(...)
    ```
    """

    @itlp.source(
        dataset=BIGQUERY_DATASET,
    )
    def source() -> tuple[itlp.Asset, ...]:
        @itlp.asset(normalizer=DataframeNormalizer())
        def asset() -> pd.DataFrame:
            return pd.DataFrame()

        return (asset,)

    source.io = {"bigquery": BigQueryIO(project="dc-int-connectors-prd", location="eu")}

    sig = signature(source.asset.data)
    with patch.object(source.asset, "data", wraps=source.asset.data):
        # The wrapping mock has the following signature: (*args, **kwargs).
        # So we need to remove the parameters to not break the asset parameter resolution.
        source.asset.data.__signature__ = sig.replace(parameters=[])
        yield source


class TestTimePartitioning:
    @pytest.mark.functional
    def test_partitioning_config(self, source: itlp.Source, client: bigquery.Client):
        """
        Tests that the table is created with the correct partitioning config.
        """

        source.asset.name = currentframe().f_code.co_name  # current function name
        source.asset.partitioning = itlp.TimePartitionConfig(column="date")
        source.asset.data.return_value = pd.DataFrame([{"date": pd.Timestamp("2025-01-01"), "what": "ever"}])
        itlp.Pipeline(source.asset).materialize()

        table = client.get_table(f"{source.dataset}.{source.asset.name}")
        assert table.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
        assert table.time_partitioning.field == "date"

    @pytest.mark.functional
    def test_replaces_same_partition(self, source: itlp.Source, client: bigquery.Client):
        """
        Tests that the same partition can be replaced with new data.
        The same partition 2025-01-01 is materialized twice with different data.
        """

        source.asset.name = currentframe().f_code.co_name  # current function name
        source.asset.partitioning = itlp.TimePartitionConfig(column="date")

        source.asset.data.return_value = pd.DataFrame([{"date": pd.Timestamp("2025-01-01"), "what": "ever"}])
        itlp.Pipeline(source.asset).materialize(partition=itlp.TimePartition(dt.date(2025, 1, 1)))

        source.asset.data.return_value = pd.DataFrame([{"date": pd.Timestamp("2025-01-01"), "what": "else"}])
        itlp.Pipeline(source.asset).materialize(partition=itlp.TimePartition(dt.date(2025, 1, 1)))

        data = fetch_data(client, f"{source.dataset}.{source.asset.name}")
        assert_frame_equal(
            data, pd.DataFrame([{"date": pd.Timestamp("2025-01-01"), "what": "else"}]), check_dtype=False
        )
