import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer


@itlp.source(normalizer=DataframeNormalizer())
def source_x(
    client_id: str = itlp.Env("CLIENT_ID"),
    client_secret: str = itlp.Env("CLIENT_SECRET"),
    refresh_token: str = itlp.Env("REFRESH_TOKEN"),
) -> tuple[itlp.Asset, ...]:
    @itlp.asset
    def asset_x() -> pd.DataFrame:
        return pd.DataFrame()

    return (asset_x,)
