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

/**
 * Toast payload for a 409 in-use delete failure, or `null` for any other
 * error (callers fall back to their generic failure toast).
 */
export function inUseToast(e: unknown, entity: string): { title: string, description: string, color: 'error' } | null {
    const usedBy = usedByFromError(e)
    if (!usedBy) return null
    return {
        title: `${entity} is in use`,
        description: `Used by: ${usedByNames(usedBy)}. Rebind or delete those first.`,
        color: 'error',
    }
}
