import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer


@itlp.source(normalizer=DataframeNormalizer())
def easybill(
    api_key: str = itlp.Env("EASYBILL_API_KEY"),
) -> tuple[itlp.Asset, ...]:
    client = itlp.RESTClient(
        base_url="https://api.easybill.de/rest/v1",
        auth=itlp.HTTPBearerAuth(api_key),
        paginator=itlp.PageNumberPaginator(total_pages_path="pages"),
    )

    @itlp.asset
    def customers() -> pd.DataFrame:
        data = []
        for page in client.paginate("/customers"):
            data.extend(page["items"])
        return pd.DataFrame(data)

    @itlp.asset
    def documents() -> pd.DataFrame:
        data = []
        for page in client.paginate("/documents"):
            data.extend(page["items"])
        return pd.DataFrame(data)

    @itlp.asset
    def document_payments() -> pd.DataFrame:
        data = []
        for page in client.paginate("/document-payments"):
            data.extend(page["items"])
        return pd.DataFrame(data)

    @itlp.asset
    def positions() -> pd.DataFrame:
        data = []
        for page in client.paginate("/positions"):
            data.extend(page["items"])
        return pd.DataFrame(data)

    @itlp.asset
    def position_groups() -> pd.DataFrame:
        data = []
        for page in client.paginate("/position-groups"):
            data.extend(page["items"])
        return pd.DataFrame(data)

    return (customers, documents, document_payments, positions, position_groups)
