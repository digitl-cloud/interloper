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

interface SourceRow {
    source: CoverageSource
    days: (CoverageDay | undefined)[]
    missing: number
    failed: number
}

interface TypeGroup {
    key: string
    name: string
    rows: SourceRow[]
    days: (DayAggregate | undefined)[]
    missing: number
    failed: number
    troubled: number
}

const dates = computed(() => windowDates(props.coverage))

function missingOn(day: DayAggregate | undefined): boolean {
    return !!day && day.covered + day.failed < day.expected
}

function byTrouble(a: { failed: number, missing: number }, b: { failed: number, missing: number }): number {
    return b.failed - a.failed || b.missing - a.missing
}

const groups = computed<TypeGroup[]>(() => {
    const rowsByKey = new Map<string, SourceRow[]>()
    for (const source of props.coverage.sources) {
        if (props.typeFilter !== 'all' && source.key !== props.typeFilter) continue
        const days = dates.value.map(date => sourceDay(source, date) ?? undefined)
        const row = { source, days, missing: days.filter(missingOn).length, failed: days.filter(day => day?.failed).length }
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
                missing: days.filter(missingOn).length,
                failed: days.filter(day => day?.failed).length,
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

/** The latest failed run behind a row's most recent failed day. */
function failedRunId(row: SourceRow): string | null {
    for (let i = row.days.length - 1; i >= 0; i--) {
        const id = row.days[i]?.failed_run_id
        if (id) return id
    }
    return null
}

/** The stretch of missing days around the selected day, else the most recent one. */
function missingRange(row: SourceRow): { start: string, end: string } {
    const selected = dates.value.indexOf(props.date)
    let end = missingOn(row.days[selected]) ? selected : row.days.findLastIndex(missingOn)
    let start = end
    while (start > 0 && missingOn(row.days[start - 1])) start--
    while (end < row.days.length - 1 && missingOn(row.days[end + 1])) end++
    return { start: dates.value[start]!, end: dates.value[end]! }
}

const runTarget = ref<ComponentRecord | null>(null)
const runRange = ref<{ start: string, end: string } | undefined>()
const runOpen = ref(false)
function run(row: SourceRow) {
    const target = componentsStore.byId(row.source.id)
    if (!target) return
    runTarget.value = target
    runRange.value = missingRange(row)
    runOpen.value = true
}

const ROW = 'grid grid-cols-[1.5rem_minmax(0,16rem)_minmax(0,1fr)_9rem_6.5rem] items-center gap-3 border-t border-default py-2 text-sm'
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
                <span>{{ windowLabel }}</span>
                <span class="text-right">Days missing · failed</span>
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
                        <span class="shrink-0 text-xs text-muted">
                            {{ group.troubled ? `${group.troubled} of ${group.rows.length} with gaps` : `${group.rows.length} complete` }}
                        </span>
                    </span>
                    <OverviewCoverageStrip :dates="dates"
                                           :days="group.days"
                                           :selected="date"
                                           @select="emit('select', $event)" />
                    <span class="text-right text-xs tabular-nums text-muted">{{ group.missing }} · {{ group.failed }}</span>
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
                        <OverviewCoverageStrip :dates="dates"
                                               :days="row.days"
                                               :selected="date"
                                               @select="emit('select', $event)" />
                        <span class="text-right text-xs tabular-nums text-muted">{{ row.missing }} · {{ row.failed }}</span>
                        <span class="text-right">
                            <ULink v-if="row.failed && failedRunId(row)"
                                   :to="{ path: `/executions/runs/${failedRunId(row)}`, query: { status: 'failed' } }"
                                   class="inline-flex items-center gap-1 text-xs text-error hover:underline">
                                Open run
                                <UIcon name="i-lucide-arrow-right"
                                       class="size-3" />
                            </ULink>
                            <UButton v-else-if="row.missing && editor"
                                     icon="i-lucide-play"
                                     label="Run"
                                     size="xs"
                                     color="neutral"
                                     variant="outline"
                                     @click="run(row)" />
                            <span v-else-if="!row.missing && !row.failed"
                                  class="inline-flex items-center gap-1 text-xs text-success">
                                <UIcon name="i-lucide-check"
                                       class="size-3" />
                                Complete
                            </span>
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
                            :initial-range="runRange" />
    </div>
</template>
