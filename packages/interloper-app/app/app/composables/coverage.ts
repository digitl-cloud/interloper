import type { MaybeRefOrGetter } from 'vue'
import type { Coverage } from '~/types/overview'

export interface DayAggregate {
    expected: number
    covered: number
    failed: number
}

/** Design cell colours; the amber tints are the partial states. */
export const CELL = {
    covered: { light: '#1fa463', dark: '#45bc84' },
    partial: { light: '#f2b23e', dark: '#e9ac46' },
    partialLow: { light: '#f9dca0', dark: '#8a6a2a' },
    failed: { light: '#e5484d', dark: '#ea686c' },
    failedLow: { light: '#f4a5a8', dark: '#a04448' },
    empty: { light: '#f4f4f5', dark: '#27272a' },
}

/** Status index order: a day's cell takes the colour at its `cellStatus`. */
export const CELL_ORDER = ['empty', 'failedLow', 'failed', 'partialLow', 'partial', 'covered'] as const

/** What a day's jobs delivered, as an index into `CELL_ORDER`. */
export function cellStatus(day: DayAggregate | undefined): 0 | 1 | 2 | 3 | 4 | 5 {
    if (!day) return 0
    if (day.failed > 0) return day.failed / day.expected >= 0.5 ? 2 : 1
    const ratio = day.covered / day.expected
    if (ratio >= 1) return 5
    return ratio >= 0.6 ? 4 : 3
}

/** Sum the coverage rows of the selected jobs per date, and phrase the window's summary. */
export function useCoverageCalendar(
    coverage: MaybeRefOrGetter<Coverage | null>,
    jobFilter: MaybeRefOrGetter<string>,
) {
    const byDate = computed(() => {
        const map = new Map<string, DayAggregate>()
        const filter = toValue(jobFilter)
        for (const day of toValue(coverage)?.days ?? []) {
            if (day.expected <= 0 || (filter !== 'all' && day.job_id !== filter)) continue
            const agg = map.get(day.date) ?? { expected: 0, covered: 0, failed: 0 }
            agg.expected += day.expected
            agg.covered += day.covered
            agg.failed += day.failed
            map.set(day.date, agg)
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
