import type { BreadcrumbItem } from '@nuxt/ui'

/**
 * First breadcrumb item styled as the page title (eyebrow design).
 * translate-y-px drops the 11px uppercase label onto the baseline of the
 * adjacent 14px crumbs; the negative margin swallows the letter-spacing's
 * trailing space so the gap around the separator stays symmetric.
 */
export function titleCrumb(label: string, to?: string): BreadcrumbItem {
    return { label, to, class: 'eyebrow text-primary translate-y-px -mr-[0.14em]' }
}

/** Breadcrumb item for an entity, rendered as an EntityBadge by PageBreadcrumb. */
export function entityCrumb(label: string, icon: string): BreadcrumbItem {
    return { label, icon, slot: 'entity' }
}
