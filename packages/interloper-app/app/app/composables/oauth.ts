/**
 * OAuth2 popup flow composables.
 *
 * Provides helpers for:
 * - Building provider-specific authorization URLs
 * - Opening a popup window for the OAuth consent screen
 * - Exchanging the authorization code for tokens via the backend
 * - Communicating the outcome back from the popup to the opener
 *
 * Opener and popup talk over a BroadcastChannel (same-origin by construction)
 * rather than `window.opener.postMessage`: providers that respond with
 * `Cross-Origin-Opener-Policy: same-origin` sever the opener link, which would
 * silently drop the result and leave the opener waiting forever.
 */

/** Rejection raised when the popup is closed before the flow completes. */
export class OAuthCancelledError extends Error {}

const OAUTH_CHANNEL = 'interloper-oauth'
const POPUP_POLL_INTERVAL_MS = 500
/** Delay between the popup closing and rejecting, so an in-flight result message wins the race. */
const POPUP_CLOSE_GRACE_MS = 1_000

/**
 * Extract a human-readable detail from a failed token-exchange request.
 *
 * The backend returns `{ detail: "..." }` on error (surfaced by ofetch as
 * `error.data`); fall back to the raw message when the shape is unexpected.
 */
export function oauthErrorDetail(error: unknown): string {
    const e = error as { data?: { detail?: unknown }, message?: string }
    const detail = e?.data?.detail
    if (typeof detail === 'string' && detail) return detail
    if (detail != null) return JSON.stringify(detail, null, 2)
    return e?.message || 'Unknown error'
}

/**
 * Composable for the OAuth2 popup sign-in flow.
 */
export function useOAuthPopup() {
    const { apiFetch } = useApi()

    /**
     * Open a popup to the given authorization URL and wait for the callback
     * page to report the outcome over the OAuth channel.
     *
     * Rejects with `OAuthCancelledError` when the popup is closed before the
     * flow completes, and with a regular `Error` when the flow itself fails.
     */
    async function signIn(url: string): Promise<Record<string, unknown>> {
        const width = 500
        const height = 600
        const left = window.screen.width / 2 - width / 2
        const top = window.screen.height / 2 - height / 2

        // Correlates this flow instance with its popup: providers echo `state`
        // back to the callback page, which includes it in the result message.
        const state = crypto.randomUUID()
        const popupUrl = new URL(url)
        popupUrl.searchParams.set('state', state)

        const popup = window.open(
            popupUrl.toString(),
            'oauth2popup',
            `width=${width},height=${height},left=${left},top=${top}`,
        )
        if (!popup) throw new Error('Failed to open popup window')

        return new Promise((resolve, reject) => {
            const channel = new BroadcastChannel(OAUTH_CHANNEL)
            let settled = false

            function settle(complete: () => void) {
                if (settled) return
                settled = true
                clearInterval(poll)
                channel.close()
                complete()
            }

            // There is no close event for a (cross-origin) popup, so poll.
            const poll = setInterval(() => {
                if (!popup.closed) return
                clearInterval(poll)
                setTimeout(
                    () => settle(() => reject(new OAuthCancelledError('Sign-in window was closed'))),
                    POPUP_CLOSE_GRACE_MS,
                )
            }, POPUP_POLL_INTERVAL_MS)

            channel.onmessage = (event: MessageEvent<OAuthPopupMessage>) => {
                const message = event.data
                if (message?.type !== OAuthPopupStatus.Success && message?.type !== OAuthPopupStatus.Failure) return
                // A mismatched state belongs to another flow instance. A missing
                // one means the provider dropped the param — accept rather than hang.
                if (message.state && message.state !== state) return

                if (message.type === OAuthPopupStatus.Success) {
                    popup.close()
                    settle(() => resolve(message.tokens))
                }
                else {
                    // Leave the popup open — it displays the failure details.
                    settle(() => reject(new Error(message.error || 'OAuth sign-in failed')))
                }
            }
        })
    }

    /**
     * Exchange an authorization code for tokens (called from the callback page).
     */
    async function exchangeCode(provider: string, code: string): Promise<Record<string, unknown>> {
        return await apiFetch<Record<string, unknown>>(`/oauth/${provider}`, {
            method: 'POST',
            body: { code },
        })
    }

    /**
     * Report the flow outcome to the opener (called from the callback page).
     */
    function reportResult(message: OAuthPopupMessage) {
        const channel = new BroadcastChannel(OAUTH_CHANNEL)
        channel.postMessage(message)
        channel.close()
    }

    return { signIn, exchangeCode, reportResult }
}

/**
 * Composable for building provider-specific OAuth2 authorization URLs.
 *
 * Uses the specs store for provider availability and credentials.
 */
export function useOAuthProvider() {
    const catalogStore = useCatalogStore()

    /**
     * Build the authorization URL for a given provider and scope.
     */
    function getAuthUrl(provider: OAuthProviderKey, scope?: string): string | null {
        const info = catalogStore.getOAuthProvider(provider)
        if (!info || !info.auth_url) return null

        const url = new URL(info.auth_url)

        switch (provider) {
            case 'google':
                url.searchParams.set('client_id', info.client_id)
                url.searchParams.set('redirect_uri', info.redirect_uri)
                url.searchParams.set('response_type', 'code')
                url.searchParams.set('access_type', 'offline')
                url.searchParams.set('prompt', 'consent')
                url.searchParams.set('flowName', 'GeneralOAuthFlow')
                if (scope) url.searchParams.set('scope', scope)
                break

            case 'microsoft':
                url.searchParams.set('client_id', info.client_id)
                url.searchParams.set('redirect_uri', info.redirect_uri)
                url.searchParams.set('response_type', 'code')
                url.searchParams.set('response_mode', 'query')
                if (scope) url.searchParams.set('scope', scope)
                break

            case 'tiktok':
                url.searchParams.set('app_id', info.client_id)
                url.searchParams.set('redirect_uri', info.redirect_uri)
                break

            default:
                // Standard OAuth2: amazon, criteo, facebook, linkedin, pinterest, snapchat
                url.searchParams.set('client_id', info.client_id)
                url.searchParams.set('redirect_uri', info.redirect_uri)
                url.searchParams.set('response_type', 'code')
                if (scope) url.searchParams.set('scope', scope)
                break
        }

        return url.toString()
    }

    return { getAuthUrl }
}
