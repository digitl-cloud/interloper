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
    /** The OAuth client id: an https URL for a client described by its metadata document. */
    id: string
    name: string
    /** The host that served the client's metadata document; null for a client that registered. */
    client_host: string | null
    redirect_uris: string[]
    created_at: string | null
}

/**
 * What the consent page shows before the user decides. `client_name` is self-declared and unverified;
 * `client_host` is the host that served the client's metadata document, proven by reading it, or null.
 */
export interface ConsentRequest {
    client_name: string
    client_host: string | null
    redirect_host: string
    resource: 'api' | 'mcp'
}
