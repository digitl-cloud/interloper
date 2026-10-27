export interface PersonalAccessToken {
    id: string
    name: string
    token_prefix: string
    created_at: string | null
    expires_at: string | null
    last_used_at: string | null
    revoked_at: string | null
}

/** Creation response — the only place the raw token ever appears. */
export interface CreatedToken extends PersonalAccessToken {
    token: string
}

/** An application acting for the user through OAuth: one holding a live refresh token. */
export interface AuthorisedApp {
    /** The OAuth client id. */
    id: string
    name: string
    redirect_uris: string[]
    created_at: string | null
}

/** What the consent page shows before the user decides. `client_name` is self-declared and unverified. */
export interface ConsentRequest {
    client_name: string
    redirect_host: string
    resource: 'api' | 'mcp'
}
