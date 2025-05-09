import logging

import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer
from interloper_sql import SQLiteIO
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.threading import ThreadingInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

itlp.basic_logging(logging.INFO)


ThreadingInstrumentor().instrument()
batch_span_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces"))
resource = Resource(attributes={"service.name": "interloper"})
trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(batch_span_processor)
trace.set_tracer_provider(trace_provider)


@itlp.source(normalizer=DataframeNormalizer())
def my_source() -> tuple[itlp.Asset, ...]:
    @itlp.asset
    def my_asset_A() -> pd.DataFrame:
        return pd.DataFrame({"a": [1, 2, 3]})

    @itlp.asset
    def my_asset_B() -> pd.DataFrame:
        return pd.DataFrame({"b": [4, 5, 6]})

    return (my_asset_A, my_asset_B)


test = my_source(
    io={
        "file": itlp.FileIO(base_dir="./data"),
        "sqlite": SQLiteIO(db_path="data/sqlite.db"),
    },
)


itlp.Pipeline(test).materialize()

trace_provider.shutdown()
