/**
 * OAuth2 popup flow composables.
 *
 * Provides helpers for:
 * - Building provider-specific authorization URLs
 * - Opening a popup window for the OAuth consent screen
 * - Exchanging the authorization code for tokens via the backend
 * - Communicating tokens back from the popup to the opener
 */


/** Wait for a single DOM event, returned as a promise. */
function promiseFromEvent<T extends Event>(target: EventTarget, event: string): Promise<T> {
    return new Promise((resolve) => {
        const handler = (e: Event) => {
            target.removeEventListener(event, handler)
            resolve(e as T)
        }
        target.addEventListener(event, handler)
    })
}

/**
 * Composable for the OAuth2 popup sign-in flow.
 */
export function useOAuthPopup() {
    const { apiFetch } = useApi()

    /**
     * Open a popup to the given authorization URL and wait for
     * the callback page to post tokens back via `postMessage`.
     */
    async function signIn(url: string): Promise<Record<string, unknown>> {
        const width = 500
        const height = 600
        const left = window.screen.width / 2 - width / 2
        const top = window.screen.height / 2 - height / 2

        const popup = window.open(
            url,
            'oauth2popup',
            `width=${width},height=${height},left=${left},top=${top}`,
        )
        if (!popup) throw new Error('Failed to open popup window')

        // Wait for the popup to post a message with our status type
        let event: MessageEvent
        do {
            event = await promiseFromEvent<MessageEvent>(window, 'message')
        } while (
            !event.data?.type
            || !Object.values(OAuthPopupStatus).includes(event.data.type)
        )

        if (event.data.type !== OAuthPopupStatus.Success) {
            throw new Error('OAuth sign-in failed')
        }

        popup.close()
        return event.data.tokens as Record<string, unknown>
    }

    /**
     * Exchange an authorization code for tokens (called from the callback page).
     * Posts the result back to the opener window.
     */
    async function exchangeCode(provider: string, code: string) {
        const tokens = await apiFetch<Record<string, unknown>>(`/oauth/${provider}`, {
            method: 'POST',
            body: { code },
        })

        if (window.opener) {
            window.opener.postMessage({
                type: OAuthPopupStatus.Success,
                tokens,
            }, '*')
        }

        return tokens
    }

    return { signIn, exchangeCode }
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
