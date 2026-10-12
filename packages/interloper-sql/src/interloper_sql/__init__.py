"""Interloper SQL integration: portable views, and a SQLAlchemy-backed destination behind the postgres extra."""

from interloper_sql.view import IbisView, ibis_view, union

try:
    from interloper_sql.connection import SQLConnection
    from interloper_sql.destination import SQLDestination
except ModuleNotFoundError as error:
    # the destination and its connection need the postgres extra; the views do not
    if error.name != "sqlalchemy":
        raise
    __all__ = [
        "IbisView",
        "ibis_view",
        "union",
    ]
else:
    __all__ = [
        "IbisView",
        "SQLConnection",
        "SQLDestination",
        "ibis_view",
        "union",
    ]
