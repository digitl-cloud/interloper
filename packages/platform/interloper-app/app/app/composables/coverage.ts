import type { MaybeRefOrGetter } from 'vue'
import type { Coverage, CoverageDay, CoverageDays, CoverageSource } from '~/types/overview'

export interface DayAggregate {
    successful: number
    failed: number
    missing: number
    missed: number
}

/** A day's partitions: its successful, failed and missing ones, never its missed snapshots. */
export function partitionCount(day: DayAggregate): number {
    return day.successful + day.failed + day.missing
}

/**
 * Cell colours as CSS expressions of the status tokens: the low states mix the
 * colour into the panel, a successful day that missed snapshots darkens it.
 * DOM cells bind them as styles; the canvas calendar resolves them with `useChartTheme`.
 */
export const CELL = {
    successful: 'var(--ui-success)',
    successfulMissed: 'color-mix(in oklab, var(--ui-success) 70%, var(--ui-text-highlighted))',
    partial: 'var(--ui-warning)',
    partialLow: 'color-mix(in oklab, var(--ui-warning) 40%, var(--ui-bg))',
    failed: 'var(--ui-error)',
    failedLow: 'color-mix(in oklab, var(--ui-error) 40%, var(--ui-bg))',
    empty: 'var(--ui-bg-elevated)',
}

/** Status index order: a day's cell takes the colour at its `cellStatus`. */
export const CELL_ORDER = ['empty', 'failedLow', 'failed', 'partialLow', 'partial', 'successful', 'successfulMissed'] as const

/**
 * What a day's sources delivered, as an index into `CELL_ORDER`. A day that only missed snapshots is neutral; a
 * successful day that also missed some takes the darker green, so the miss shows without counting among its partitions.
 */
export function cellStatus(day: DayAggregate | undefined): 0 | 1 | 2 | 3 | 4 | 5 | 6 {
    const total = day ? partitionCount(day) : 0
    if (!day || !total) return 0
    if (day.failed > 0) return day.failed / total >= 0.5 ? 2 : 1
    const ratio = day.successful / total
    if (ratio >= 1) return day.missed ? 6 : 5
    return ratio >= 0.6 ? 4 : 3
}

export type PartitionState = 'successful' | 'missing' | 'failed' | 'missed'

/** How each partition state is drawn wherever it is counted: the source list's badges and a day's breakdown. */
export const PARTITION_STATES: {
    key: PartitionState
    color: string
    badge: StatusColor
    label: string
}[] = [
    { key: 'successful', color: 'text-success', badge: 'success', label: 'successful' },
    { key: 'missing', color: 'text-warning', badge: 'warning', label: 'missing' },
    { key: 'failed', color: 'text-error', badge: 'error', label: 'failed' },
    { key: 'missed', color: 'text-muted', badge: 'neutral', label: 'missed' },
]

/** A partition state's icon, from the icon maps. Call it where `useAppConfig` is available (setup, render). */
export function partitionStateIcon(state: PartitionState): string {
    const { ui, icons } = useAppConfig()
    switch (state) {
        case 'successful': return ui.icons.success
        case 'missing': return icons.missing
        case 'failed': return ui.icons.error
        case 'missed': return icons.missed
    }
}

/** The states with a count, in `PARTITION_STATES` order, each with its icon, count and the words that name it. */
export function stateCounts(counts: Record<PartitionState, number>) {
    return PARTITION_STATES
        .filter(state => counts[state.key] > 0)
        .map(state => ({
            ...state,
            icon: partitionStateIcon(state.key),
            value: counts[state.key],
            text: `${counts[state.key].toLocaleString()} ${counts[state.key] === 1 ? 'partition' : 'partitions'} ${state.label}`,
        }))
}

/** What a coverage window with nothing to count says, wherever it says it. */
export const NO_PARTITIONS = 'No partitions in this window'

const DAY_MS = 86_400_000

/** An ISO date as epoch milliseconds at midnight UTC. */
function epochDay(date: string): number {
    return Date.parse(`${date}T00:00:00Z`)
}

/** Every date of a coverage window, oldest first. */
export function windowDates(coverage: Coverage): string[] {
    const dates: string[] = []
    for (let day = epochDay(coverage.since); day <= epochDay(coverage.until); day += DAY_MS)
        dates.push(new Date(day).toISOString().slice(0, 10))
    return dates
}

/** The index of a date in a run of days' arrays. */
function dayIndex(days: CoverageDays, date: string): number {
    return Math.round((epochDay(date) - epochDay(days.start)) / DAY_MS)
}

/** A run of days' day on one date, unrolled from its arrays; `null` when it has neither partitions nor misses then. */
export function coverageDay(days: CoverageDays, date: string): DayAggregate | null {
    const i = dayIndex(days, date)
    const day = {
        successful: days.successful[i] ?? 0,
        failed: days.failed[i] ?? 0,
        missing: days.missing[i] ?? 0,
        missed: days.missed[i] ?? 0,
    }
    return partitionCount(day) || day.missed ? day : null
}

/** A source's day on one date, with the run that failed it; `null` when it has neither partitions nor misses then. */
export function sourceDay(source: CoverageSource, date: string): CoverageDay | null {
    const day = coverageDay(source, date)
    if (!day) return null
    return { ...day, date, source_id: source.id, failed_run_id: source.failed_run_ids[String(dayIndex(source, date))] ?? null }
}

/** Sum the days of the selected type, or of every type, per date. */
export function useCoverageCalendar(
    coverage: MaybeRefOrGetter<Coverage | null>,
    typeFilter: MaybeRefOrGetter<string>,
) {
    const byDate = computed(() => {
        const map = new Map<string, DayAggregate>()
        const filter = toValue(typeFilter)
        for (const type of toValue(coverage)?.types ?? []) {
            if (filter !== 'all' && type.key !== filter) continue
            const start = epochDay(type.start)
            type.successful.forEach((successful, i) => {
                const failed = type.failed[i] ?? 0
                const missing = type.missing[i] ?? 0
                const missed = type.missed[i] ?? 0
                if (!successful && !failed && !missing && !missed) return
                const date = new Date(start + i * DAY_MS).toISOString().slice(0, 10)
                const agg = map.get(date) ?? { successful: 0, failed: 0, missing: 0, missed: 0 }
                agg.successful += successful
                agg.failed += failed
                agg.missing += missing
                agg.missed += missed
                map.set(date, agg)
            })
        }
        return map
    })

    return { byDate }
}
