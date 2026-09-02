/** One referencing component in a 409 in-use error payload. */
export interface UsedByRef {
    id: string
    kind: string
    key: string
    name: string | null
}

/**
 * Extract the `used_by` referrer list from a DELETE 409 response
 * (`{detail: {message, used_by}}`), or `null` for any other error.
 */
export function usedByFromError(e: unknown): UsedByRef[] | null {
    const err = e as { status?: number, statusCode?: number, data?: { detail?: { used_by?: UsedByRef[] } } }
    const usedBy = err?.data?.detail?.used_by
    return (err?.status ?? err?.statusCode) === 409 && Array.isArray(usedBy) && usedBy.length ? usedBy : null
}

/** Human-readable list of referrer names for a toast description. */
export function usedByNames(refs: UsedByRef[]): string {
    return refs.map(r => r.name ?? r.key).join(', ')
}

/** Toast payload naming an entity's referrers (pre-flight checks and 409 handling share the copy). */
export function inUseToastPayload(entity: string, refs: UsedByRef[]): { title: string, description: string, color: 'error' } {
    return {
        title: `${entity} is in use`,
        description: `Used by: ${usedByNames(refs)}. Rebind or delete those first.`,
        color: 'error',
    }
}

/**
 * Toast payload for a 409 in-use delete failure, or `null` for any other
 * error (callers fall back to their generic failure toast).
 */
export function inUseToast(e: unknown, entity: string): { title: string, description: string, color: 'error' } | null {
    const usedBy = usedByFromError(e)
    return usedBy ? inUseToastPayload(entity, usedBy) : null
}

/**
 * Human-readable message carried by an API error, or `null` when it has none.
 *
 * Handles the three FastAPI `detail` shapes — a plain string, a `{message, …}`
 * object (409 in-use), and a 422 validation list — plus plain `Error`s from
 * non-HTTP flows (e.g. the OAuth popup) whose `message` is already
 * user-facing. HTTP errors without a usable `detail` yield `null` rather than
 * ofetch's unhelpful `[POST] "/api/…": 400` message.
 */
export function errorDetail(e: unknown): string | null {
    const detail = (e as { data?: { detail?: unknown } })?.data?.detail
    if (typeof detail === 'string' && detail) return detail
    if (Array.isArray(detail)) {
        const msgs = detail
            .map(item => (item as { msg?: unknown })?.msg)
            .filter((msg): msg is string => typeof msg === 'string')
        if (msgs.length) return msgs.join('\n')
    }
    const message = (detail as { message?: unknown } | null)?.message
    if (typeof message === 'string' && message) return message
    if (e instanceof Error && e.message && !('response' in e)) return e.message
    return null
}

/** Stands in for an error whose detail is not fit to show (or absent). */
export const GENERIC_ERROR = 'Something went wrong. Try again in a moment.'

/** Failure toast payload: `title` as the headline, the error's detail (when any) as the description. */
export function errorToast(e: unknown, title: string): { title: string, description?: string, color: 'error' } {
    return { title, description: errorDetail(e) ?? undefined, color: 'error' }
}
