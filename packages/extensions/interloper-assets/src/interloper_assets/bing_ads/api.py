"""The Microsoft Advertising API engine every Bing Ads request goes through."""

from __future__ import annotations

import asyncio
import datetime as dt
import tempfile
from collections.abc import Sequence
from functools import cached_property
from typing import Any
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

import httpx2
import pandas as pd

from interloper_assets.bing_ads import constants


class BingAdsApi:
    """Microsoft Advertising: Customer Management over SOAP, Reporting and Bulk through the bingads SDK.

    Customer Management is spoken as raw SOAP over httpx2: fetch-field
    providers run in the API process, which installs the connection classes
    but not the SDK. The SDK is imported by the methods that need it, each
    through the package it lives in: threads entering it by different
    submodules deadlock on its import locks.

    The SDK answers a refused report or Bulk download with a generic
    ``Invalid client data`` fault; the operation errors in its detail are
    raised instead. Customer Management's refusal is its HTTP error.
    """

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, developer_token: str) -> None:
        """Build the engine on the connection's credentials.

        Args:
            client_id: The OAuth application's client id.
            client_secret: The OAuth application's client secret.
            refresh_token: The refresh token every access token is exchanged for.
            developer_token: The developer token every call carries.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.developer_token = developer_token

    # -- Public API ------------------------------------------------------------

    async def call(self, action: str, entity: str, **params: Any) -> list[dict[str, Any]]:
        """Call one Customer Management operation and read the entities it answers with.

        Each call exchanges the refresh token for the access token it carries.

        Args:
            action: The operation, such as ``GetAccount``.
            entity: The name of the entities to read from the answer, such as ``AdvertiserAccount``.
            **params: The request's fields by their Bing names; see `_element` for how each is encoded.

        Returns:
            Every entity named ``entity`` in the answer, in order, as a dict of its fields: a field's
            text, None for a nil field, or one dict per record for a field holding records.
        """
        async with httpx2.AsyncClient(timeout=30) as client:
            access_token = await self._access_token(client)
            response = await client.post(
                constants.CUSTOMER_MANAGEMENT_URL,
                content=self._envelope(action, access_token, params),
                headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": action},
            )
            response.raise_for_status()
        answer = ET.fromstring(response.text)
        return [self._record(element) for element in answer.iter() if self._local_name(element.tag) == entity]

    async def report(
        self,
        account_id: str,
        day: dt.date,
        request: str,
        *,
        scope: str,
        columns: list[str],
        scope_fields: dict[str, Any] | None = None,
        percentages: Sequence[str] = (),
        **params: Any,
    ) -> pd.DataFrame:
        """Run one report of the account over one day and read its rows.

        The report is asked for as CSV with its column headers and without
        Bing's header and footer rows; the blocking SDK download runs in a
        thread.

        Args:
            account_id: The account the report is authorized for and covers.
            day: The report's only day.
            request: The report request type, such as ``AdPerformanceReportRequest``; its columns
                are of the matching ``...ReportColumn`` type.
            scope: The report scope type, such as ``AccountThroughAdGroupReportScope``; its
                ``AccountIds`` is the account.
            columns: The report's columns.
            scope_fields: The scope's other fields by their Bing names, such as ``Campaigns``; a field
                left out keeps the SDK factory's default.
            percentages: The columns Bing writes as percentage strings (``"1.50%"``), read without
                their ``%``; a name the report lacks is ignored.
            **params: The request's other fields by their Bing names, such as ``Aggregation``.

        Returns:
            One row per report row under Bing's column names, numbers read with their thousands
            separators; an empty frame when the report has no rows.
        """
        return await asyncio.to_thread(
            self._report_rows,
            account_id,
            day,
            request,
            scope,
            columns,
            scope_fields or {},
            percentages,
            params,
        )

    async def bulk(self, account_id: str, **params: Any) -> pd.DataFrame:
        """Download a Bulk file of the account's entities and read its records.

        The Bulk service also authorizes on the account's parent customer,
        read from the account first; the blocking SDK download then runs in a
        thread.

        Args:
            account_id: The account whose entities are downloaded.
            **params: The SDK's ``DownloadParameters`` naming what is downloaded, such as
                ``download_entities`` and ``data_scope``.

        Returns:
            One row per record, under the Bulk file's column names; see `_bulk_records`.
        """
        customer_id = await self._customer_id(account_id)
        return await asyncio.to_thread(self._bulk_records, account_id, customer_id, params)

    # -- Customer Management ---------------------------------------------------

    async def _access_token(self, client: httpx2.AsyncClient) -> str:
        """Exchange the refresh token for an access token a SOAP call carries.

        Args:
            client: The HTTP client the exchange is made with.

        Returns:
            The access token.
        """
        response = await client.post(
            constants.TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": constants.OAUTH_SCOPE,
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]

    def _envelope(self, action: str, access_token: str, params: dict[str, Any]) -> str:
        """Wrap an operation's request in the SOAP envelope Customer Management reads.

        Args:
            action: The operation, which names the request element ``<action>Request``.
            access_token: The access token the call carries.
            params: The request's fields by their Bing names.

        Returns:
            The envelope, signed with the developer token and the access token.
        """
        fields = "".join(self._element(name, value) for name, value in params.items())
        return (
            '<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" '
            'xmlns:i="http://www.w3.org/2001/XMLSchema-instance">'
            f'<s:Header xmlns="{constants.CUSTOMER_NAMESPACE}">'
            f'<Action mustUnderstand="1">{action}</Action>'
            f"<DeveloperToken>{self.developer_token}</DeveloperToken>"
            f"<AuthenticationToken>{access_token}</AuthenticationToken>"
            "</s:Header>"
            f'<s:Body><{action}Request xmlns="{constants.CUSTOMER_NAMESPACE}">{fields}</{action}Request></s:Body>'
            "</s:Envelope>"
        )

    @classmethod
    def _element(cls, name: str, value: Any, prefix: str = "") -> str:
        """Encode one request field.

        None is a nil field and a list repeats the field once per item. A
        dict is an entity whose fields are in Customer Management's entities
        namespace, declared on the request field that holds it. A boolean is
        ``true`` or ``false``, a day or an instant its ISO form, and any other
        value its text.

        Args:
            name: The field's Bing name.
            value: The field's value.
            prefix: ``a:`` inside an entity, so the field is in the entities namespace.

        Returns:
            The field as XML, its text escaped.
        """
        if isinstance(value, list):
            return "".join(cls._element(name, item, prefix) for item in value)
        if value is None:
            return f'<{prefix}{name} i:nil="true"/>'
        if isinstance(value, dict):
            namespace = "" if prefix else f' xmlns:a="{constants.ENTITIES_NAMESPACE}"'
            fields = "".join(cls._element(field, item, "a:") for field, item in value.items())
            return f"<{prefix}{name}{namespace}>{fields}</{prefix}{name}>"
        if isinstance(value, bool):
            text = "true" if value else "false"
        elif isinstance(value, dt.date):
            text = value.isoformat()
        else:
            text = str(value)
        return f"<{prefix}{name}>{escape(text)}</{prefix}{name}>"

    @classmethod
    def _record(cls, entity: ET.Element) -> dict[str, Any]:
        """Read an entity of an answer as a dict of its fields.

        A field with child elements holds records, such as an account's
        ``LinkedAgencies`` listing ``CustomerInfo`` records, read as one dict
        per record. A nil field has no text and reads as None.

        Args:
            entity: The entity's element.

        Returns:
            The entity's fields by their Bing names.
        """
        return {
            cls._local_name(field.tag): (
                field.text
                if len(field) == 0
                else [{cls._local_name(item.tag): item.text for item in record} for record in field]
            )
            for field in entity
        }

    @staticmethod
    def _local_name(tag: str) -> str:
        """Strip the ``{namespace}`` prefix ElementTree puts on every tag.

        Args:
            tag: The tag as ElementTree reads it.

        Returns:
            The bare tag name.
        """
        return tag.rsplit("}", 1)[-1]

    async def _customer_id(self, account_id: str) -> str:
        """Read the customer an account belongs to, which the Bulk service authorizes on.

        Args:
            account_id: The account whose customer is read.

        Returns:
            The account's parent customer id.

        Raises:
            RuntimeError: If Bing returns no account, or an account naming no parent customer.
        """
        accounts = await self.call("GetAccount", "Account", AccountId=int(account_id))
        if not accounts:
            raise RuntimeError(f"Bing Ads returned no account for {account_id}")
        customer_id = accounts[0].get("ParentCustomerId")
        if not customer_id:
            raise RuntimeError(f"Bing Ads account {account_id} names no parent customer")
        return customer_id

    # -- Reporting and Bulk ----------------------------------------------------

    @cached_property
    def _authentication(self) -> Any:
        """The SDK's OAuth authentication, exchanging the refresh token once per engine.

        Returns:
            The SDK's ``OAuthAuthorization``.
        """
        from bingads import OAuthAuthorization, OAuthWebAuthCodeGrant

        grant = OAuthWebAuthCodeGrant(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirection_uri=None,
        )
        oauth_tokens = grant.request_oauth_tokens_by_refresh_token(self.refresh_token)
        return OAuthAuthorization(client_id=grant.client_id, oauth_tokens=oauth_tokens)

    def _authorization(self, account_id: str, customer_id: str | None = None) -> Any:
        """Authorize an SDK service for one account.

        Args:
            account_id: The account every call is made for.
            customer_id: The account's parent customer, which the Bulk service requires; None leaves it
                unset, where the SDK would otherwise send the string ``"None"``.

        Returns:
            The SDK's ``AuthorizationData``.
        """
        from bingads import AuthorizationData

        authorization = AuthorizationData(developer_token=self.developer_token, authentication=self._authentication)
        authorization.account_id = account_id
        authorization.customer_id = customer_id
        return authorization

    def _report_rows(
        self,
        account_id: str,
        day: dt.date,
        request: str,
        scope: str,
        columns: list[str],
        scope_fields: dict[str, Any],
        percentages: Sequence[str],
        params: dict[str, Any],
    ) -> pd.DataFrame:
        """Run one report and read its rows, blocking; see `report`.

        Args:
            account_id: The account the report is authorized for and covers.
            day: The report's only day.
            request: The report request type.
            scope: The report scope type.
            columns: The report's columns.
            scope_fields: The scope's other fields.
            percentages: The columns read without their ``%``.
            params: The request's other fields.

        Returns:
            The report's rows; see `report`.
        """
        from bingads import ServiceClient
        from bingads.v13.reporting import ReportingDownloadParameters, ReportingServiceManager

        service = ServiceClient("ReportingService", 13, self._authorization(account_id))
        column = request.removesuffix("Request") + "Column"
        report_columns = service.factory.create(f"ArrayOf{column}")
        getattr(report_columns, column).append(columns)
        report_request = self._create(
            service,
            request,
            Scope=self._create(service, scope, AccountIds={"long": [account_id]}, **scope_fields),
            Columns=report_columns,
            Time=self._report_time(service, day),
            Format="Csv",
            ExcludeColumnHeaders=False,
            ExcludeReportHeader=True,
            ExcludeReportFooter=True,
            **params,
        )
        manager = ReportingServiceManager(
            self._authorization(account_id),
            working_directory=tempfile.mkdtemp(prefix="bingads-"),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = self._download(
                manager,
                ReportingDownloadParameters(
                    report_request=report_request,
                    result_file_directory=directory,
                    result_file_name="report.csv",
                    overwrite_result_file=True,
                    timeout_in_milliseconds=constants.DOWNLOAD_TIMEOUT,
                ),
            )
            # the SDK returns no path when the report has no rows for the day
            if not path:
                return pd.DataFrame()
            try:
                df = pd.read_csv(path, thousands=",", encoding="utf-8-sig")
            except pd.errors.EmptyDataError:
                return pd.DataFrame()

        for name in percentages:
            if name in df.columns:
                df[name] = df[name].astype("string").str.rstrip("%")
        return df

    def _report_time(self, service: Any, day: dt.date) -> Any:
        """Build a report's time: the one day, bounded by a fixed time zone.

        Bing dates each row in its campaign's time zone whatever the request
        names: ``ReportTimeZone`` only decides which zone's today the end date
        may not pass. One fixed zone therefore serves every account.

        Args:
            service: The Reporting service whose factory builds the request objects.
            day: The report's only day.

        Returns:
            The SDK's ``ReportTime``.
        """
        return self._create(
            service,
            "ReportTime",
            CustomDateRangeStart=self._create(service, "Date", Day=day.day, Month=day.month, Year=day.year),
            CustomDateRangeEnd=self._create(service, "Date", Day=day.day, Month=day.month, Year=day.year),
            PredefinedTime=None,
            ReportTimeZone=constants.REPORT_TIME_ZONE,
        )

    def _bulk_records(self, account_id: str, customer_id: str, params: dict[str, Any]) -> pd.DataFrame:
        """Download a Bulk file and read its records, blocking; see `bulk`.

        The file is read as the Bulk API writes it, every value as text: an
        id never passes through a float. Its header is shared by every record
        type, so the columns the downloaded records leave empty are dropped,
        and its file-level rows (format version, account) are not records.
        Three serializations are read here, since the conformer would misread
        them: booleans are ``True`` and ``False``, days are month first
        (``11/05/2026``, or ``11/05/2026 00:00:00`` on a campaign record,
        whose day is the part before the time), and instants are month first
        too, in UTC.

        Args:
            account_id: The account whose entities are downloaded.
            customer_id: The account's parent customer.
            params: The SDK's ``DownloadParameters`` naming what is downloaded.

        Returns:
            One row per record, under the Bulk file's column names.
        """
        from bingads.v13.bulk import BulkServiceManager, DownloadParameters

        manager = BulkServiceManager(
            self._authorization(account_id, customer_id),
            working_directory=tempfile.mkdtemp(prefix="bingads-"),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = self._download(
                manager,
                DownloadParameters(
                    result_file_directory=directory,
                    result_file_name="entities.csv",
                    overwrite_result_file=True,
                    file_type="Csv",
                    timeout_in_milliseconds=constants.DOWNLOAD_TIMEOUT,
                    **params,
                ),
            )
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")

        df = df[~df["Type"].isin(constants.BULK_FILE_RECORDS)].dropna(axis="columns", how="all")
        for name in df.columns:
            if df[name].dropna().str.lower().isin(["true", "false"]).all():
                df[name] = df[name].str.lower().map({"true": True, "false": False})
        for name in ("Start Date", "End Date"):
            if name in df.columns:
                df[name] = pd.to_datetime(df[name], format="%m/%d/%Y", exact=False).dt.date
        if "Modified Time" in df.columns:
            df["Modified Time"] = pd.to_datetime(df["Modified Time"], format="mixed", utc=True)
        return df.reset_index(drop=True)

    @staticmethod
    def _create(service: Any, type_name: str, **fields: Any) -> Any:
        """Create an SDK request object and set its fields.

        Args:
            service: The SDK service whose factory creates the object.
            type_name: The object's SOAP type, such as ``ReportTime``.
            **fields: The fields to set, by their Bing names; the others keep the factory's defaults.

        Returns:
            The object.
        """
        created = service.factory.create(type_name)
        for name, value in fields.items():
            setattr(created, name, value)
        return created

    @staticmethod
    def _download(manager: Any, parameters: Any) -> Any:
        """Download a report or Bulk file, raising a SOAP fault as Bing's own errors.

        The SDK surfaces a generic ``Invalid client data`` fault; the
        actionable reason (``AccountNotAuthorized``) lives in its detail as
        operation, batch or API errors. A fault naming none, and any other
        error, is raised as is.

        Args:
            manager: The SDK's Reporting or Bulk service manager.
            parameters: The SDK's download parameters.

        Returns:
            What the manager's ``download_file`` returns: the file's path, or None for a report without rows.

        Raises:
            RuntimeError: If Bing refused the download, naming each error's code and message.
            WebFault: If Bing refused the download naming no error, as the SDK raised it.
        """
        from suds import WebFault

        try:
            return manager.download_file(parameters)
        except WebFault as fault:
            detail = getattr(fault.fault, "detail", None)
            errors: list[str] = []
            for fault_name in ("ApiFaultDetail", "AdApiFaultDetail"):
                fault_detail = getattr(detail, fault_name, None)
                if not fault_detail:
                    continue
                for group_name, item_name in (
                    ("OperationErrors", "OperationError"),
                    ("BatchErrors", "BatchError"),
                    ("Errors", "AdApiError"),
                ):
                    group = getattr(fault_detail, group_name, None)
                    if not group or isinstance(group, str):
                        continue
                    items = getattr(group, item_name, [])
                    for item in items if isinstance(items, list) else [items]:
                        code = getattr(item, "ErrorCode", None) or getattr(item, "Code", None)
                        message = getattr(item, "Message", "")
                        errors.append(f"{code}: {message}" if code else str(message))
            if errors:
                raise RuntimeError("Bing Ads API error -- " + "; ".join(errors)) from fault
            raise
