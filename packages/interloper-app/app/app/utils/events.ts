import type { EventType } from '~/types/event'

type BadgeColor = 'error' | 'success' | 'info' | 'warning' | 'neutral'

const iconMap: Record<EventType, string> = {
    asset_queued: 'i-lucide-clock',
    asset_skipped: 'i-lucide-skip-forward',
    asset_started: 'i-lucide-play-circle',
    asset_completed: 'i-lucide-check-circle',
    asset_failed: 'i-lucide-alert-circle',
    asset_canceled: 'i-lucide-x-circle',
    asset_exec_started: 'i-lucide-play-circle',
    asset_exec_completed: 'i-lucide-check-circle',
    asset_exec_failed: 'i-lucide-alert-circle',
    dest_read_started: 'i-lucide-download',
    dest_read_completed: 'i-lucide-download-cloud',
    dest_read_failed: 'i-lucide-x-circle',
    dest_write_started: 'i-lucide-upload',
    dest_write_completed: 'i-lucide-upload-cloud',
    dest_write_failed: 'i-lucide-x-circle',
    run_dispatched: 'i-lucide-send',
    run_started: 'i-lucide-rocket',
    run_completed: 'i-lucide-flag',
    run_failed: 'i-lucide-alert-circle',
    backfill_started: 'i-lucide-rewind',
    backfill_completed: 'i-lucide-flag',
    backfill_failed: 'i-lucide-alert-circle',
    log: 'i-lucide-align-left',
}

const labelMap: Record<EventType, string> = {
    asset_queued: 'Asset Queued',
    asset_skipped: 'Asset Skipped',
    asset_started: 'Asset Started',
    asset_completed: 'Asset Completed',
    asset_failed: 'Asset Failed',
    asset_canceled: 'Asset Canceled',
    asset_exec_started: 'Asset Exec Started',
    asset_exec_completed: 'Asset Exec Completed',
    asset_exec_failed: 'Asset Exec Failed',
    dest_read_started: 'Destination Read Started',
    dest_read_completed: 'Destination Read Completed',
    dest_read_failed: 'Destination Read Failed',
    dest_write_started: 'Destination Write Started',
    dest_write_completed: 'Destination Write Completed',
    dest_write_failed: 'Destination Write Failed',
    run_dispatched: 'Run Dispatched',
    run_started: 'Run Started',
    run_completed: 'Run Completed',
    run_failed: 'Run Failed',
    backfill_started: 'Backfill Started',
    backfill_completed: 'Backfill Completed',
    backfill_failed: 'Backfill Failed',
    log: 'Log',
}

export function eventTypeIcon(eventType: EventType): string {
    return iconMap[eventType] ?? 'i-lucide-align-left'
}

export function eventTypeLabel(eventType: EventType): string {
    return labelMap[eventType] ?? eventType
}

const levelColorMap: Record<string, BadgeColor> = {
    DEBUG: 'neutral',
    INFO: 'info',
    WARNING: 'warning',
    ERROR: 'error',
    CRITICAL: 'error',
}

export function logLevelColor(level: string): BadgeColor {
    return levelColorMap[level.toUpperCase()] ?? 'neutral'
}

export function eventTypeColor(eventType: EventType): BadgeColor {
    if (eventType.includes('failed')) return 'error'
    if (eventType.includes('completed')) return 'success'
    if (eventType.includes('started')) return 'info'
    if (eventType.includes('dispatched')) return 'info'
    if (eventType.includes('skipped')) return 'warning'
    if (eventType.includes('canceled')) return 'warning'
    if (eventType.includes('queued')) return 'neutral'
    return 'neutral'
}

