/** Design avatar palette — deterministic per key so colors are stable across sessions. */
export const AVATAR_PALETTE = ['#10B6CB', '#6C5CE7', '#1FA463', '#E69E2E', '#2D7DF6', '#E5484D', '#C8511B']

/** Stable palette color for an entity (member, organisation), hashed from its key. */
export function avatarColor(key: string): string {
    let hash = 0
    for (let i = 0; i < key.length; i++) hash = (hash * 31 + key.charCodeAt(i)) | 0
    return AVATAR_PALETTE[Math.abs(hash) % AVATAR_PALETTE.length]!
}

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
