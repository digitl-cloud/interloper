import interloper as itlp
import pandas as pd
from interloper_pandas import DataframeNormalizer


@itlp.source(normalizer=DataframeNormalizer())
def clockify(
    api_key: str = itlp.Env("CLOCKIFY_API_KEY"),
) -> tuple[itlp.Asset, ...]:
    client = itlp.RESTClient(
        base_url="https://api.clockify.me/api/v1",
        headers={"X-Api-Key": api_key},
        paginator=itlp.PageNumberPaginator(
            initial_page=1,
            page_param="page",
        ),
    )

    @itlp.asset
    def workspaces() -> pd.DataFrame:
        response = client.get("/workspaces")
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)

    @itlp.asset
    def user() -> pd.DataFrame:
        response = client.get("/user")
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame([data])

    @itlp.asset
    def users(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/users"):
            data.extend(page)
        return pd.DataFrame(data)

    @itlp.asset
    def approval_requests(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/approval-requests"):
            data.extend(page)
        return pd.DataFrame(data)

    @itlp.asset
    def clients(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/clients"):
            data.extend(page)
        return pd.DataFrame(data)

    @itlp.asset
    def custom_fields(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/custom-fields"):
            data.extend(page)
        return pd.DataFrame(data)

    @itlp.asset
    def expenses(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/expenses"):
            data.extend(page)
        return pd.DataFrame(data)

    @itlp.asset
    def expense_categories(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/expenses/categories"):
            data.extend(page)
        return pd.DataFrame(data)

    @itlp.asset
    def invoices(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/invoices"):
            data.extend(page)
        return pd.DataFrame(data)

    @itlp.asset
    def projects(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/projects"):
            data.extend(page)
        return pd.DataFrame(data)

    @itlp.asset
    def tags(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/tags"):
            data.extend(page)
        return pd.DataFrame(data)

    @itlp.asset
    def holidays(workspace_id: str) -> pd.DataFrame:
        response = client.get(f"/workspaces/{workspace_id}/holidays")
        response.raise_for_status()
        return pd.DataFrame(response.json())

    @itlp.asset
    def shared_reports(workspace_id: str) -> pd.DataFrame:
        data = []
        for page in client.paginate(f"/workspaces/{workspace_id}/shared-reports"):
            data.extend(page)
        return pd.DataFrame(data)

    return (
        user,
        workspaces,
        users,
        approval_requests,
        clients,
        custom_fields,
        expenses,
        expense_categories,
        invoices,
        projects,
        tags,
        holidays,
        shared_reports,
    )
