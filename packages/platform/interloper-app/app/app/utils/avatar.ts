/** Up-to-two-letter initials from a display name, falling back to an email/e-mail-like string. */
export function getInitials(name: string | null | undefined, fallback?: string | null): string {
    const trimmed = name?.trim()
    if (trimmed) {
        const parts = trimmed.split(/\s+/)
        const first = parts[0]?.[0] ?? ''
        const last = parts.length > 1 ? (parts[parts.length - 1]?.[0] ?? '') : ''
        return (last ? first + last : first).toUpperCase()
    }
    return fallback?.charAt(0).toUpperCase() ?? '?'
}
