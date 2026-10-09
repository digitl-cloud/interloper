<script setup lang="ts">
import type { ComponentRecord } from '~/types/component'
import type { Coverage, CoverageDay, CoverageSource } from '~/types/overview'
import type { DayAggregate } from '~/composables/coverage'

const props = defineProps<{
    date: string
    coverage: Coverage
    typeFilter: string
}>()

const emit = defineEmits<{ select: [date: string] }>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const editor = useCanEdit()

/** A window's owed days, each counted once by its worst state: failed, else missing, else covered. */
interface DayTally {
    covered: number
    missing: number
    failed: number
}

interface SourceRow extends DayTally {
    source: CoverageSource
    days: (CoverageDay | undefined)[]
}

interface TypeGroup extends DayTally {
    key: string
    name: string
    rows: SourceRow[]
    days: (DayAggregate | undefined)[]
    troubled: number
}

const dates = computed(() => windowDates(props.coverage))

function missingOn(day: DayAggregate | undefined): boolean {
    return !!day && day.covered + day.failed < day.expected
}

function tally(days: (DayAggregate | undefined)[]): DayTally {
    const counts = { covered: 0, missing: 0, failed: 0 }
    for (const day of days) {
        if (!day) continue
        if (day.failed) counts.failed++
        else if (missingOn(day)) counts.missing++
        else counts.covered++
    }
    return counts
}

function dayCounts(counts: DayTally) {
    return [
        { key: 'covered', icon: 'i-lucide-circle-check', color: 'text-success', label: 'covered', value: counts.covered },
        { key: 'missing', icon: 'i-lucide-circle-dashed', color: 'text-warning', label: 'missing', value: counts.missing },
        { key: 'failed', icon: 'i-lucide-circle-alert', color: 'text-error', label: 'failed', value: counts.failed },
    ]
}

function byTrouble(a: { failed: number, missing: number }, b: { failed: number, missing: number }): number {
    return b.failed - a.failed || b.missing - a.missing
}

const groups = computed<TypeGroup[]>(() => {
    const rowsByKey = new Map<string, SourceRow[]>()
    for (const source of props.coverage.sources) {
        if (props.typeFilter !== 'all' && source.key !== props.typeFilter) continue
        const days = dates.value.map(date => sourceDay(source, date) ?? undefined)
        const row = { source, days, ...tally(days) }
        rowsByKey.set(source.key, [...(rowsByKey.get(source.key) ?? []), row])
    }
    return [...rowsByKey.entries()]
        .map(([key, rows]) => {
            const days = dates.value.map((_, i) => {
                const owed = rows.flatMap(row => row.days[i] ?? [])
                if (!owed.length) return undefined
                return {
                    expected: owed.reduce((n, day) => n + day.expected, 0),
                    covered: owed.reduce((n, day) => n + day.covered, 0),
                    failed: owed.reduce((n, day) => n + day.failed, 0),
                }
            })
            return {
                key,
                name: catalogStore.typeName(key),
                rows: rows.sort((a, b) => byTrouble(a, b) || a.source.name.localeCompare(b.source.name)),
                days,
                ...tally(days),
                troubled: rows.filter(row => row.missing || row.failed).length,
            }
        })
        .sort((a, b) => byTrouble(a, b) || a.name.localeCompare(b.name))
})

const troubledGroups = computed(() => groups.value.filter(group => group.troubled))
const completeGroups = computed(() => groups.value.filter(group => !group.troubled))
const completeSources = computed(() => completeGroups.value.reduce((n, group) => n + group.rows.length, 0))
const showComplete = ref(false)
const visibleGroups = computed(() => showComplete.value ? [...troubledGroups.value, ...completeGroups.value] : troubledGroups.value)

const expanded = ref(new Set<string>())
watch(() => groups.value.map(group => group.key).join(), () => {
    showComplete.value = !troubledGroups.value.length && groups.value.length === 1
}, { immediate: true })

function toggle(key: string) {
    const next = new Set(expanded.value)
    if (!next.delete(key)) next.add(key)
    expanded.value = next
}

const summary = computed(() => {
    const owed = groups.value.flatMap(group => group.rows.flatMap(row => row.days[dates.value.indexOf(props.date)] ?? []))
    if (!owed.length) return 'Nothing expected on this day'
    const expected = owed.reduce((n, day) => n + day.expected, 0)
    const covered = owed.reduce((n, day) => n + day.covered, 0)
    const failed = owed.reduce((n, day) => n + day.failed, 0)
    const parts = [`${covered} of ${expected} assets covered`]
    if (failed) parts.push(`${failed} failed`)
    if (covered + failed < expected) parts.push(`${expected - covered - failed} missing`)
    return parts.join(' · ')
})

const label = computed(() => new Date(`${props.date}T00:00:00Z`).toLocaleDateString(undefined, {
    weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
}))

const windowLabel = computed(() => {
    const { since, until } = props.coverage
    const year = since.slice(0, 4) !== until.slice(0, 4) ? 'numeric' : undefined
    const format = (date: string) => new Date(`${date}T00:00:00Z`).toLocaleDateString(undefined, {
        day: 'numeric', month: 'short', year, timeZone: 'UTC',
    })
    return `${format(since)} → ${format(until)}`
})

const selectedIndex = computed(() => dates.value.indexOf(props.date))

/** A row's day on the selected date, which its actions act on. */
function selectedDay(row: SourceRow): CoverageDay | undefined {
    return row.days[selectedIndex.value]
}

const runTarget = ref<ComponentRecord | null>(null)
const runOpen = ref(false)
function run(row: SourceRow) {
    const target = componentsStore.byId(row.source.id)
    if (!target) return
    runTarget.value = target
    runOpen.value = true
}

const ROW = 'grid grid-cols-[1.5rem_minmax(0,16rem)_10rem_minmax(0,1fr)_6.5rem] items-center gap-3 border-t border-default py-2 text-sm'
</script>

<template>
    <div class="mt-5">
        <div class="mb-2.5 flex flex-wrap items-baseline gap-2.5">
            <span class="text-sm font-semibold text-highlighted">{{ label }}</span>
            <span class="text-xs text-muted">{{ summary }}</span>
        </div>
        <template v-if="groups.length">
            <div :class="ROW"
                 class="border-t-0 text-xs text-muted">
                <span />
                <span>Source</span>
                <span>Days</span>
                <span>{{ windowLabel }}</span>
                <span />
            </div>
            <template v-for="group in visibleGroups"
                      :key="group.key">
                <div :class="ROW">
                    <UButton :icon="expanded.has(group.key) ? 'i-lucide-chevron-down' : 'i-lucide-chevron-right'"
                             :aria-label="expanded.has(group.key) ? `Collapse ${group.name}` : `Expand ${group.name}`"
                             size="xs"
                             color="neutral"
                             variant="ghost"
                             @click="toggle(group.key)" />
                    <span class="flex min-w-0 items-center gap-2">
                        <UIcon :name="componentIcon(group.key)"
                               class="size-5 shrink-0" />
                        <span class="truncate font-medium text-highlighted">{{ group.name }}</span>
                    </span>
                    <span class="flex gap-2.5 text-xs tabular-nums">
                        <span v-for="count in dayCounts(group)"
                              :key="count.key"
                              :title="`${count.value} ${count.value === 1 ? 'day' : 'days'} ${count.label}`"
                              class="inline-flex items-center gap-1"
                              :class="count.value ? count.color : 'text-dimmed'">
                            <UIcon :name="count.icon"
                                   class="size-3.5" />
                            {{ count.value.toLocaleString() }}
                        </span>
                    </span>
                    <OverviewCoverageStrip :dates="dates"
                                           :days="group.days"
                                           :selected="date"
                                           @select="emit('select', $event)" />
                    <span />
                </div>
                <template v-if="expanded.has(group.key)">
                    <div v-for="row in group.rows"
                         :key="row.source.id"
                         :class="ROW">
                        <span />
                        <span class="flex min-w-0 items-center gap-1.5 pl-2">
                            <UIcon :name="row.source.kind === 'asset' ? 'i-lucide-box' : 'i-lucide-plug'"
                                   class="size-3.5 shrink-0 text-dimmed" />
                            <span class="truncate font-mono text-xs text-highlighted">{{ row.source.name }}</span>
                        </span>
                        <span class="flex gap-2.5 text-xs tabular-nums">
                            <span v-for="count in dayCounts(row)"
                                  :key="count.key"
                                  :title="`${count.value} ${count.value === 1 ? 'day' : 'days'} ${count.label}`"
                                  class="inline-flex items-center gap-1"
                                  :class="count.value ? count.color : 'text-dimmed'">
                                <UIcon :name="count.icon"
                                       class="size-3.5" />
                                {{ count.value.toLocaleString() }}
                            </span>
                        </span>
                        <OverviewCoverageStrip :dates="dates"
                                               :days="row.days"
                                               :selected="date"
                                               @select="emit('select', $event)" />
                        <span class="text-right">
                            <UButton v-if="selectedDay(row)?.failed_run_id"
                                     label="Open run"
                                     trailing-icon="i-lucide-arrow-right"
                                     size="xs"
                                     color="neutral"
                                     variant="outline"
                                     :to="{ path: `/executions/runs/${selectedDay(row)?.failed_run_id}`, query: { status: 'failed' } }" />
                            <UButton v-else-if="missingOn(selectedDay(row)) && editor"
                                     icon="i-lucide-play"
                                     label="Run"
                                     size="xs"
                                     color="neutral"
                                     variant="outline"
                                     @click="run(row)" />
                        </span>
                    </div>
                </template>
            </template>
            <div v-if="completeGroups.length && troubledGroups.length"
                 class="flex items-center gap-2 border-t border-default py-2.5 text-xs text-muted">
                <UIcon name="i-lucide-check"
                       class="size-3.5 text-success" />
                <span>{{ completeGroups.length }} {{ completeGroups.length === 1 ? 'type' : 'types' }} complete · {{ completeSources }} {{ completeSources === 1 ? 'source' : 'sources' }}</span>
                <UButton :label="showComplete ? 'Hide' : 'Show'"
                         size="xs"
                         color="neutral"
                         variant="link"
                         class="p-0"
                         @click="showComplete = !showComplete" />
            </div>
            <div v-else-if="!troubledGroups.length && !showComplete"
                 class="flex items-center gap-2 border-t border-default py-2.5 text-xs text-muted">
                <UIcon name="i-lucide-check"
                       class="size-3.5 text-success" />
                <span>Every source is complete in this window · {{ completeSources }} {{ completeSources === 1 ? 'source' : 'sources' }}</span>
                <UButton label="Show"
                         size="xs"
                         color="neutral"
                         variant="link"
                         class="p-0"
                         @click="showComplete = true" />
            </div>
        </template>
        <p v-else
           class="text-sm text-muted">
            Nothing expected in this window
        </p>
        <ExecutionsRunModal v-if="runTarget"
                            v-model:open="runOpen"
                            :target="runTarget"
                            :initial-range="{ start: date, end: date }" />
    </div>
</template>
