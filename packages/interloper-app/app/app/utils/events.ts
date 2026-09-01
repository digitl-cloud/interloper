import type { EventType } from '~/types/event'

type BadgeColor = 'error' | 'success' | 'info' | 'warning' | 'neutral'

const iconMap: Record<EventType, string> = {
    operation_queued: 'i-lucide-clock',
    operation_skipped: 'i-lucide-skip-forward',
    operation_started: 'i-lucide-play-circle',
    operation_completed: 'i-lucide-check-circle',
    operation_failed: 'i-lucide-alert-circle',
    operation_canceled: 'i-lucide-x-circle',
    asset_data_started: 'i-lucide-play-circle',
    asset_data_completed: 'i-lucide-check-circle',
    asset_data_failed: 'i-lucide-alert-circle',
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
    hook_fired: 'i-lucide-webhook',
    hook_failed: 'i-lucide-alert-circle',
    log: 'i-lucide-align-left',
}

const labelMap: Record<EventType, string> = {
    operation_queued: 'Operation Queued',
    operation_skipped: 'Operation Skipped',
    operation_started: 'Operation Started',
    operation_completed: 'Operation Completed',
    operation_failed: 'Operation Failed',
    operation_canceled: 'Operation Canceled',
    asset_data_started: 'Asset Data Started',
    asset_data_completed: 'Asset Data Completed',
    asset_data_failed: 'Asset Data Failed',
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
    hook_fired: 'Hook Fired',
    hook_failed: 'Hook Failed',
    log: 'Log',
}

export type EventCategory = 'all' | 'lifecycle' | 'errors' | 'logs'

/** All known event types, derived from the icon map so this can't drift. */
const ALL_EVENT_TYPES = Object.keys(iconMap) as EventType[]

/**
 * High-level orchestration milestones (run/operation/backfill state machine).
 * Excludes the granular `asset_data_*` / `dest_*` IO events and `log`.
 */
const LIFECYCLE_TYPES: EventType[] = [
    'run_dispatched',
    'run_started',
    'run_completed',
    'run_failed',
    'operation_queued',
    'operation_skipped',
    'operation_started',
    'operation_completed',
    'operation_failed',
    'operation_canceled',
    'backfill_started',
    'backfill_completed',
    'backfill_failed',
    'hook_fired',
    'hook_failed',
]

/** Every failure event, derived so new `*_failed` types are picked up automatically. */
const ERROR_TYPES: EventType[] = ALL_EVENT_TYPES.filter(t => t.endsWith('_failed'))

/** Event types backing a category tab; `all` returns null (no filter). */
export function eventTypesForCategory(category: EventCategory): EventType[] | null {
    switch (category) {
        case 'lifecycle': return LIFECYCLE_TYPES
        case 'errors': return ERROR_TYPES
        case 'logs': return ['log']
        default: return null
    }
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

