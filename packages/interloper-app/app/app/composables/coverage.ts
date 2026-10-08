import type { MaybeRefOrGetter } from 'vue'
import type { Coverage, CoverageDay, CoverageSource } from '~/types/overview'

export interface DayAggregate {
    expected: number
    covered: number
    failed: number
}

/** Cell colours from Tailwind's status scales; the low states are their tints. */
export const CELL = {
    covered: { light: '#00bc7d', dark: '#00d492' },
    partial: { light: '#ffb900', dark: '#ffb900' },
    partialLow: { light: '#fee685', dark: '#8d630e' },
    failed: { light: '#fb2c36', dark: '#ff6467' },
    failedLow: { light: '#ffa2a2', dark: '#8d4041' },
    empty: { light: '#f5f5f5', dark: '#262626' },
}

/** Status index order: a day's cell takes the colour at its `cellStatus`. */
export const CELL_ORDER = ['empty', 'failedLow', 'failed', 'partialLow', 'partial', 'covered'] as const

/** What a day's sources delivered, as an index into `CELL_ORDER`. */
export function cellStatus(day: DayAggregate | undefined): 0 | 1 | 2 | 3 | 4 | 5 {
    if (!day) return 0
    if (day.failed > 0) return day.failed / day.expected >= 0.5 ? 2 : 1
    const ratio = day.covered / day.expected
    if (ratio >= 1) return 5
    return ratio >= 0.6 ? 4 : 3
}

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

/** A source's day on one date, unrolled from its arrays; `null` when nothing is expected of it then. */
export function sourceDay(source: CoverageSource, date: string): CoverageDay | null {
    const i = Math.round((epochDay(date) - epochDay(source.start)) / DAY_MS)
    const expected = source.expected[i] ?? 0
    if (expected <= 0) return null
    return {
        date,
        source_id: source.id,
        expected,
        covered: source.covered[i] ?? 0,
        failed: source.failed[i] ?? 0,
        failed_run_id: source.failed_run_ids[String(i)] ?? null,
    }
}

/** Sum the days of the sources of the selected type per date, and phrase the window's summary. */
export function useCoverageCalendar(
    coverage: MaybeRefOrGetter<Coverage | null>,
    typeFilter: MaybeRefOrGetter<string>,
) {
    const byDate = computed(() => {
        const map = new Map<string, DayAggregate>()
        const filter = toValue(typeFilter)
        for (const source of toValue(coverage)?.sources ?? []) {
            if (filter !== 'all' && source.key !== filter) continue
            const start = epochDay(source.start)
            source.expected.forEach((expected, i) => {
                if (expected <= 0) return
                const date = new Date(start + i * DAY_MS).toISOString().slice(0, 10)
                const agg = map.get(date) ?? { expected: 0, covered: 0, failed: 0 }
                agg.expected += expected
                agg.covered += source.covered[i] ?? 0
                agg.failed += source.failed[i] ?? 0
                map.set(date, agg)
            })
        }
        return map
    })

    const summary = computed(() => {
        let expected = 0
        let covered = 0
        let gaps = 0
        let failures = 0
        for (const day of byDate.value.values()) {
            expected += day.expected
            covered += day.covered
            if (day.failed) failures++
            else if (day.covered < day.expected) gaps++
        }
        if (!expected) return 'Nothing expected in this window'
        const pct = Math.round((100 * covered) / expected)
        return `${pct}% of ${expected.toLocaleString()} partitions · ${gaps} days with gaps · ${failures} with failures`
    })

    return { byDate, summary }
}
