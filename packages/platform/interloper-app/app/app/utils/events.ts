import type { EventType } from '~/types/event'

const labelMap: Record<EventType, string> = {
    operation_queued: 'Operation queued',
    operation_skipped: 'Operation skipped',
    operation_started: 'Operation started',
    operation_completed: 'Operation completed',
    operation_failed: 'Operation failed',
    operation_retried: 'Operation retried',
    operation_canceled: 'Operation canceled',
    asset_data_started: 'Asset data started',
    asset_data_completed: 'Asset data completed',
    asset_data_failed: 'Asset data failed',
    dest_read_started: 'Destination read started',
    dest_read_completed: 'Destination read completed',
    dest_read_failed: 'Destination read failed',
    dest_write_started: 'Destination write started',
    dest_write_completed: 'Destination write completed',
    dest_write_failed: 'Destination write failed',
    run_dispatched: 'Run dispatched',
    run_started: 'Run started',
    run_completed: 'Run completed',
    run_failed: 'Run failed',
    backfill_started: 'Backfill started',
    backfill_completed: 'Backfill completed',
    backfill_failed: 'Backfill failed',
    hook_fired: 'Hook fired',
    hook_failed: 'Hook failed',
    log: 'Log',
}

export const EVENT_CATEGORIES = ['all', 'lifecycle', 'errors', 'logs'] as const
export type EventCategory = typeof EVENT_CATEGORIES[number]

/** All known event types, derived from the label map so this can't drift. */
const ALL_EVENT_TYPES = Object.keys(labelMap) as EventType[]

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
    'operation_retried',
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

/** The status an event's outcome reads as, by its suffix; colours and icons come from the status table. */
const OUTCOMES: [suffix: string, status: string][] = [
    ['_completed', 'success'],
    ['_failed', 'failed'],
    ['_started', 'running'],
    ['_dispatched', 'dispatched'],
    ['_queued', 'queued'],
    ['_skipped', 'skipped'],
    ['_canceled', 'canceled'],
]

function eventStatus(eventType: EventType): string | null {
    return OUTCOMES.find(([suffix]) => eventType.endsWith(suffix))?.[1] ?? null
}

const LOG_ICON = 'i-lucide-align-left'

/**
 * An event's icon. Outcomes take the status table's icon; a start shows what
 * started (a run, a destination read or write, a backfill), since a spinner
 * would claim the event is still going. Call it where `useAppConfig` is
 * available (setup, render).
 */
export function eventTypeIcon(eventType: EventType): string {
    const { ui, icons } = useAppConfig()
    switch (eventType) {
        case 'dest_read_started': return 'i-lucide-download'
        case 'dest_write_started': return ui.icons.upload
        case 'backfill_started': return icons.backfill
        case 'run_dispatched': return 'i-lucide-send'
        case 'operation_retried': return ui.icons.reload
        case 'hook_fired': return icons.hook
        case 'log': return LOG_ICON
    }
    const status = eventStatus(eventType)
    if (status === 'running') return icons.run
    return status ? statusIcon(status) : LOG_ICON
}

export function eventTypeLabel(eventType: EventType): string {
    return labelMap[eventType] ?? eventType
}

const levelColorMap: Record<string, StatusColor> = {
    DEBUG: 'neutral',
    INFO: 'info',
    WARNING: 'warning',
    ERROR: 'error',
    CRITICAL: 'error',
}

export function logLevelColor(level: string): StatusColor {
    return levelColorMap[level.toUpperCase()] ?? 'neutral'
}

/** An event's colour: its outcome's status colour; a retry warns, anything else is neutral. */
export function eventTypeColor(eventType: EventType): StatusColor {
    const status = eventStatus(eventType)
    if (status) return statusMeta(status).color
    return eventType.endsWith('_retried') ? 'warning' : 'neutral'
}
