from inspect import currentframe, signature
from unittest.mock import patch

import interloper as itlp
import pandas as pd
import pytest
from google.cloud import bigquery
from interloper_google_cloud.io import BigQueryIO
from interloper_pandas.normalizer import DataframeNormalizer
from pandas.testing import assert_frame_equal

BIGQUERY_DATASET = "interloper_functional_tests"


def fetch_table_schema(client: bigquery.Client, table: str) -> dict[str, str]:
    table = client.get_table(table)
    return {str(field.name): str(field.field_type) for field in table.schema}


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


class TestFlexibleStrategy:
    @pytest.mark.functional
    def test_without_asset_schema(self, source: itlp.Source, client: bigquery.Client):
        """
        Tests that the asset schema is inferred from the data.
        """

        source.asset.name = currentframe().f_code.co_name  # current function name
        source.materialization_strategy = itlp.MaterializationStrategy.FLEXIBLE
        source.asset.data.return_value = pd.DataFrame([{"a": 0, "b": 0.0, "c": "0"}])
        itlp.Pipeline(source.asset).materialize()

        schema = fetch_table_schema(client, f"{source.dataset}.{source.asset.name}")
        assert schema == {"a": "INTEGER", "b": "FLOAT", "c": "STRING"}

        data = fetch_data(client, f"{source.dataset}.{source.asset.name}")
        assert_frame_equal(data, pd.DataFrame([{"a": 0, "b": 0.0, "c": "0"}]), check_dtype=False)

    @pytest.mark.functional
    def test_with_asset_schema(self, source: itlp.Source, client: bigquery.Client):
        """
        Tests that the asset schema is used if provided.
        Then the data types are reconciled to the match the table schema.
        """

        source.asset.name = currentframe().f_code.co_name  # current function name
        source.materialization_strategy = itlp.MaterializationStrategy.FLEXIBLE

        class Schema(itlp.AssetSchema):
            a: float
            b: float
            c: str

        source.asset.schema = Schema
        source.asset.data.return_value = pd.DataFrame([{"a": 0, "b": 0.0, "c": "0"}])
        itlp.Pipeline(source.asset).materialize()

        schema = fetch_table_schema(client, f"{source.dataset}.{source.asset.name}")
        data = fetch_data(client, f"{source.dataset}.{source.asset.name}")
        assert schema == {"a": "FLOAT", "b": "FLOAT", "c": "STRING"}
        assert_frame_equal(data, pd.DataFrame([{"a": 0.0, "b": 0.0, "c": "0"}]), check_dtype=False)

    @pytest.mark.functional
    def test_mixed_typed_data_being_reconciled(self, source: itlp.Source, client: bigquery.Client):
        """
        Tests that the data types are reconciled when the data is mixed typed.
        Here the schema will be first inferred as float during the first materialization.
        Then integer and string will be reconciled to float during the second materialization.
        """

        source.asset.name = currentframe().f_code.co_name  # current function name
        source.materialization_strategy = itlp.MaterializationStrategy.FLEXIBLE

        source.asset.data.return_value = pd.DataFrame([{"a": 0.0, "b": 0.0}])
        itlp.Pipeline(source.asset).materialize()

        source.asset.data.return_value = pd.DataFrame([{"a": 0, "b": "0"}])
        itlp.Pipeline(source.asset).materialize()

        data = fetch_data(client, f"{source.dataset}.{source.asset.name}")
        assert_frame_equal(data, pd.DataFrame([{"a": 0.0, "b": 0.0}, {"a": 0.0, "b": 0.0}]), check_dtype=False)


class TestStrictStrategy:
    @pytest.mark.functional
    def test_static_vs_inferred_schema_match(self, source: itlp.Source, client: bigquery.Client):
        """
        Tests that everything goes smoothly if the static schema matches the inferred schema.
        """

        source.asset.name = currentframe().f_code.co_name  # current function name
        source.materialization_strategy = itlp.MaterializationStrategy.STRICT

        class Schema(itlp.AssetSchema):
            a: int
            b: float
            c: str

        source.asset.schema = Schema
        source.asset.data.return_value = pd.DataFrame([{"a": 0, "b": 0.0, "c": "0"}])
        itlp.Pipeline(source.asset).materialize()

    @pytest.mark.functional
    def test_static_vs_inferred_schema_mismatch(self, source: itlp.Source, client: bigquery.Client):
        """
        Tests that an error is raised if the static schema does not match the inferred schema.
        """

        source.asset.name = currentframe().f_code.co_name  # current function name
        source.materialization_strategy = itlp.MaterializationStrategy.STRICT

        class Schema(itlp.AssetSchema):
            a: int
            b: float
            c: str

        source.asset.schema = Schema
        source.asset.data.return_value = pd.DataFrame([{"a": 0.0, "b": 0.0, "c": "0"}])
        with pytest.raises(itlp.errors.AssetNormalizationError):
            itlp.Pipeline(source.asset).materialize()
