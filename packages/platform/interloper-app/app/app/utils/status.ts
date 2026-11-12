/**
 * The one status table. Every display of a run, backfill or execution status —
 * badges, dots, progress segments, timeline bars, chart colours, legends —
 * derives from it, so a status reads the same everywhere.
 */

/** The semantic colour a status takes. */
export type StatusColor = 'success' | 'error' | 'info' | 'warning' | 'neutral'

export interface StatusMeta {
    label: string
    color: StatusColor
    /** Still executing: badges spin, dots pulse. */
    executing: boolean
}

const STATUSES: Record<string, StatusMeta> = {
    success: { label: 'Success', color: 'success', executing: false },
    failed: { label: 'Failed', color: 'error', executing: false },
    running: { label: 'Running', color: 'info', executing: true },
    dispatched: { label: 'Dispatched', color: 'info', executing: true },
    queued: { label: 'Queued', color: 'neutral', executing: false },
    pending: { label: 'Pending', color: 'neutral', executing: false },
    canceled: { label: 'Canceled', color: 'warning', executing: false },
    skipped: { label: 'Skipped', color: 'neutral', executing: false },
}

/** Label, colour and executing flag of a status; an unknown status reads as neutral. */
export function statusMeta(status?: string | null): StatusMeta {
    return (status && STATUSES[status]) || {
        label: status ? status.charAt(0).toUpperCase() + status.slice(1) : '—',
        color: 'neutral',
        executing: false,
    }
}

/** The icon of a status, from the app's icon maps. Call it where `useAppConfig` is available (setup, render). */
export function statusIcon(status?: string | null): string {
    const { ui, icons } = useAppConfig()
    switch (status) {
        case 'success': return ui.icons.success
        case 'failed': return ui.icons.error
        case 'running':
        case 'dispatched': return ui.icons.loading
        case 'queued':
        case 'pending': return icons.queued
        case 'canceled': return icons.cancel
        case 'skipped': return icons.skipped
        default: return icons.status
    }
}

/** Background class of a status dot, swatch or bar segment, per colour. */
export const STATUS_BG: Record<StatusColor, string> = {
    success: 'bg-success',
    error: 'bg-error',
    info: 'bg-info',
    warning: 'bg-warning',
    neutral: 'bg-(--ui-text-dimmed)',
}

/** The same colours as CSS values, for style bindings, `UProgressGroup` and canvas (resolved by `useChartTheme`). */
export const STATUS_CSS: Record<StatusColor, string> = {
    success: 'var(--ui-success)',
    error: 'var(--ui-error)',
    info: 'var(--ui-info)',
    warning: 'var(--ui-warning)',
    neutral: 'var(--ui-text-dimmed)',
}
