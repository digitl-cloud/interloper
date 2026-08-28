"""Shared FastAPI dependencies: application state, identity, and role gates.

Three concerns, one import surface. ``state`` holds what ``create_app``
installs once at startup; ``auth`` resolves the caller and their active
organisation per request; ``rbac`` gates handlers on the caller's role.
"""

from interloper_api.dependencies.auth import (
    get_current_org,
    get_current_user,
    get_org_id,
    get_session_context,
)
from interloper_api.dependencies.rbac import (
    authorize_org_member,
    load_authorized,
    require_admin,
    require_editor,
    require_super_admin,
    require_viewer,
)
from interloper_api.dependencies.state import (
    get_admin_config,
    get_auth_config,
    get_catalog,
    get_features,
    get_quota_defaults,
    get_smtp_config,
    get_store,
    set_admin_config,
    set_auth_config,
    set_catalog,
    set_features,
    set_quota_defaults,
    set_smtp_config,
    set_store,
)

__all__ = [
    "authorize_org_member",
    "get_admin_config",
    "get_auth_config",
    "get_catalog",
    "get_current_org",
    "get_current_user",
    "get_features",
    "get_org_id",
    "get_quota_defaults",
    "get_session_context",
    "get_smtp_config",
    "get_store",
    "load_authorized",
    "require_admin",
    "require_editor",
    "require_super_admin",
    "require_viewer",
    "set_admin_config",
    "set_auth_config",
    "set_catalog",
    "set_features",
    "set_quota_defaults",
    "set_smtp_config",
    "set_store",
]
