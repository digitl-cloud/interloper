export interface PersonalAccessToken {
    id: string
    name: string
    token_prefix: string
    organisation_id: string
    created_at: string | null
    expires_at: string | null
    last_used_at: string | null
    revoked_at: string | null
}

/** Creation response — the only place the raw token ever appears. */
export interface CreatedToken extends PersonalAccessToken {
    token: string
}
