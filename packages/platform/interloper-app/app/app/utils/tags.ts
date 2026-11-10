import type { BadgeProps } from '@nuxt/ui'

const TAG_COLORS: NonNullable<BadgeProps['color']>[] = ['primary', 'secondary', 'info', 'success', 'warning', 'error']

/** Deterministic badge color for a tag — the same value always gets the same color. */
export function tagColor(tag: string): NonNullable<BadgeProps['color']> {
    let hash = 0
    for (let i = 0; i < tag.length; i++) hash = (hash * 31 + tag.charCodeAt(i)) | 0
    return TAG_COLORS[Math.abs(hash) % TAG_COLORS.length]!
}
