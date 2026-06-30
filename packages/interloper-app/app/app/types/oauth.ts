export type OAuthProviderKey = string

export enum OAuthPopupStatus {
    Loading = 'OAUTH_POPUP_LOADING',
    Success = 'OAUTH_POPUP_SUCCESS',
    Failure = 'OAUTH_POPUP_FAILURE',
    MissingCode = 'OAUTH_POPUP_MISSING_CODE',
}

/** Public provider info returned by the API (no secrets). */
export interface OAuthProviderInfo {
    key: string
    client_id: string
    redirect_uri: string
    auth_url: string
    label: string
    icon: string
}

/** x-oauth metadata from the JSON Schema root. */
export interface OAuthFieldMeta {
    provider: OAuthProviderKey
    auth_url: string
    scope: string
    label: string
    icon: string
    /**
     * OAuth role → model-field-name mapping (roles: client_id / client_secret /
     * refresh_token). The mapped fields are hidden in sign-in mode; the
     * refresh_token field receives the token returned by the flow.
     */
    fields: Record<string, string>
}
