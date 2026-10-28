"""Shared FastAPI dependencies: application state, identity, and role gates.

Three concerns, one import surface. ``state`` holds what ``create_app``
installs once at startup; ``auth`` resolves the caller and the organisation
the path names per request; ``rbac`` gates handlers on the caller's role there.
"""

from interloper_api.dependencies.auth import (
    CurrentUserDep,
    OrgDep,
    OrgScope,
    SessionUserDep,
    get_caller,
    get_org_scope,
    get_session_caller,
    require_run,
    session_user,
)
from interloper_api.dependencies.rbac import (
    AdminDep,
    EditorDep,
    SuperAdminDep,
    ViewerDep,
    load_authorized,
    require_admin,
    require_editor,
    require_super_admin,
    require_viewer,
)
from interloper_api.dependencies.state import (
    AgentDep,
    AuthorizationServerDep,
    CatalogDep,
    SettingsDep,
    State,
    StateDep,
    StoreDep,
    get_agent,
    get_authorization_server,
    get_catalog,
    get_settings,
    get_state,
    get_store,
)

__all__ = [
    "AdminDep",
    "AgentDep",
    "AuthorizationServerDep",
    "CatalogDep",
    "CurrentUserDep",
    "EditorDep",
    "OrgDep",
    "OrgScope",
    "SessionUserDep",
    "SettingsDep",
    "State",
    "StateDep",
    "StoreDep",
    "SuperAdminDep",
    "ViewerDep",
    "get_agent",
    "get_authorization_server",
    "get_caller",
    "get_catalog",
    "get_org_scope",
    "get_session_caller",
    "get_settings",
    "get_state",
    "get_store",
    "load_authorized",
    "require_admin",
    "require_editor",
    "require_run",
    "require_super_admin",
    "require_viewer",
    "session_user",
]
