<script setup lang="ts">
import type { ComponentRecord } from '~/types/component'
import type { Coverage, CoverageDay, CoverageSource } from '~/types/overview'
import type { DayAggregate } from '~/composables/coverage'

const organisationStore = useOrganisationStore()
const overviewStore = useOverviewStore()
const { expandedTypes, typeSources } = storeToRefs(overviewStore)

const props = defineProps<{
    date: string
    coverage: Coverage
    typeFilter: string
}>()

const emit = defineEmits<{ select: [date: string] }>()

const catalogStore = useCatalogStore()
const componentsStore = useComponentsStore()
const editor = useCanEdit()
const { ui, icons } = useAppConfig()

/** A window's partitions by state, summed over its days; missed snapshots are counted apart, never among them. */
interface DayTally {
    successful: number
    missing: number
    failed: number
    missed: number
}

interface SourceRow extends DayTally {
    source: CoverageSource
    days: (CoverageDay | undefined)[]
}

interface TypeGroup extends DayTally {
    key: string
    name: string
    days: (DayAggregate | undefined)[]
    sources: number
    troubled: number
}

const dates = computed(() => windowDates(props.coverage))

function missingOn(day: DayAggregate | undefined): boolean {
    return !!day && day.missing > 0
}

function tally(days: (DayAggregate | undefined)[]): DayTally {
    const counts = { successful: 0, missing: 0, failed: 0, missed: 0 }
    for (const day of days) {
        if (!day) continue
        counts.successful += day.successful
        counts.missing += day.missing
        counts.failed += day.failed
        counts.missed += day.missed
    }
    return counts
}

function byTrouble(a: { failed: number, missing: number }, b: { failed: number, missing: number }): number {
    return b.failed - a.failed || b.missing - a.missing
}

const groups = computed<TypeGroup[]>(() => props.coverage.types
    .filter(type => props.typeFilter === 'all' || type.key === props.typeFilter)
    .map((type) => {
        const days = dates.value.map(date => coverageDay(type, date) ?? undefined)
        const name = catalogStore.typeName(type.key)
        return { key: type.key, name, days, ...tally(days), sources: type.sources, troubled: type.troubled }
    })
    .sort((a, b) => byTrouble(a, b) || a.name.localeCompare(b.name)))

/** The rows of each expanded type whose sources have arrived, the most troubled first. */
const rowsByType = computed(() => {
    const rows = new Map<string, SourceRow[]>()
    for (const key of expandedTypes.value) {
        const sources = typeSources.value[key]?.sources
        if (!sources) continue
        rows.set(key, sources
            .map((source) => {
                const days = dates.value.map(date => sourceDay(source, date) ?? undefined)
                return { source, days, ...tally(days) }
            })
            .sort((a, b) => byTrouble(a, b) || a.source.name.localeCompare(b.source.name)))
    }
    return rows
})

const troubledGroups = computed(() => groups.value.filter(group => group.troubled))
const completeGroups = computed(() => groups.value.filter(group => !group.troubled))
const completeSources = computed(() => completeGroups.value.reduce((n, group) => n + group.sources, 0))
const showComplete = ref(false)
const visibleGroups = computed(() => showComplete.value ? [...troubledGroups.value, ...completeGroups.value] : troubledGroups.value)

watch(() => groups.value.map(group => group.key).join(), () => {
    showComplete.value = !troubledGroups.value.length && groups.value.length === 1
}, { immediate: true })

const windowLabel = computed(() => {
    const { since, until } = props.coverage
    const year = since.slice(0, 4) !== until.slice(0, 4)
    return `${formatCalendarDay(since, { year })} → ${formatCalendarDay(until, { year })}`
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

/** One row of the list; every row sizes its columns the same, so they line up as a table's would. */
const ROW = 'flex items-center gap-3 border-t border-default py-2 text-sm'
const COL = {
    toggle: 'w-8 shrink-0',
    name: 'w-64 min-w-0',
    counts: 'w-60 shrink-0',
    strip: 'min-w-0 flex-1',
    action: 'flex w-28 shrink-0 justify-end',
}
</script>

<template>
    <div class="mt-4">
        <template v-if="groups.length">
            <div :class="ROW"
                 class="border-t-0 text-xs text-muted">
                <span :class="COL.toggle" />
                <span :class="COL.name">Source</span>
                <span :class="COL.counts">Partitions</span>
                <span :class="COL.strip">{{ windowLabel }}</span>
                <span :class="COL.action" />
            </div>
            <template v-for="group in visibleGroups"
                      :key="group.key">
                <div :class="ROW">
                    <span :class="COL.toggle">
                        <UButton :icon="expandedTypes.has(group.key) ? ui.icons.chevronDown : ui.icons.chevronRight"
                                 :aria-label="expandedTypes.has(group.key) ? `Collapse ${group.name}` : `Expand ${group.name}`"
                                 size="sm"
                                 color="neutral"
                                 variant="ghost"
                                 @click="overviewStore.toggleType(group.key)" />
                    </span>
                    <span class="flex items-center gap-2"
                          :class="COL.name">
                        <UIcon :name="componentIcon(group.key)"
                               class="size-4 shrink-0" />
                        <span class="truncate font-medium text-highlighted">{{ group.name }}</span>
                    </span>
                    <OverviewCoverageCounts :counts="group"
                                            :class="COL.counts" />
                    <OverviewCoverageStrip :dates="dates"
                                           :days="group.days"
                                           :selected="date"
                                           :class="COL.strip"
                                           @select="emit('select', $event)" />
                    <span :class="COL.action" />
                </div>
                <div v-if="expandedTypes.has(group.key) && typeSources[group.key]?.error"
                     class="border-t border-default py-2">
                    <LoadError title="Couldn't load sources"
                               :error="typeSources[group.key]?.error"
                               @retry="overviewStore.fetchTypeSources(group.key)" />
                </div>
                <template v-else-if="expandedTypes.has(group.key) && !rowsByType.has(group.key)">
                    <div v-for="i in group.sources"
                         :key="i"
                         :class="ROW">
                        <span :class="COL.toggle" />
                        <span class="pl-2"
                              :class="COL.name">
                            <USkeleton class="h-4 w-40" />
                        </span>
                        <span :class="COL.counts">
                            <USkeleton class="h-5 w-24" />
                        </span>
                        <USkeleton class="h-3.5"
                                   :class="COL.strip" />
                        <span :class="COL.action" />
                    </div>
                </template>
                <template v-else-if="expandedTypes.has(group.key)">
                    <div v-for="row in rowsByType.get(group.key)"
                         :key="row.source.id"
                         :class="ROW">
                        <span :class="COL.toggle" />
                        <span class="flex items-center gap-1.5 pl-2"
                              :class="COL.name">
                            <UIcon :name="row.source.kind === 'asset' ? icons.asset : icons.source"
                                   class="size-4 shrink-0 text-dimmed" />
                            <span class="truncate text-highlighted">{{ row.source.name }}</span>
                        </span>
                        <OverviewCoverageCounts :counts="row"
                                                :class="COL.counts" />
                        <OverviewCoverageStrip :dates="dates"
                                               :days="row.days"
                                               :selected="date"
                                               :class="COL.strip"
                                               @select="emit('select', $event)" />
                        <span :class="COL.action">
                            <UButton v-if="selectedDay(row)?.failed_run_id"
                                     label="Open run"
                                     :trailing-icon="ui.icons.arrowRight"
                                     size="sm"
                                     color="neutral"
                                     variant="outline"
                                     :to="{ path: organisationStore.path(`/executions/runs/${selectedDay(row)?.failed_run_id}`), query: { status: 'failed' } }" />
                            <UButton v-else-if="missingOn(selectedDay(row)) && editor"
                                     :icon="icons.run"
                                     label="Run"
                                     size="sm"
                                     color="neutral"
                                     variant="outline"
                                     @click="run(row)" />
                        </span>
                    </div>
                </template>
            </template>
            <div v-if="completeGroups.length && troubledGroups.length"
                 class="flex items-center gap-2 border-t border-default py-2 text-xs text-muted">
                <UIcon :name="ui.icons.success"
                       class="size-3.5 text-success" />
                <span class="tabular-nums">{{ completeGroups.length }} {{ completeGroups.length === 1 ? 'type' : 'types' }} complete · {{ completeSources }} {{ completeSources === 1 ? 'source' : 'sources' }}</span>
                <UButton :label="showComplete ? 'Hide' : 'Show'"
                         size="xs"
                         color="neutral"
                         variant="link"
                         @click="showComplete = !showComplete" />
            </div>
            <div v-else-if="!troubledGroups.length && !showComplete"
                 class="flex items-center gap-2 border-t border-default py-2 text-xs text-muted">
                <UIcon :name="ui.icons.success"
                       class="size-3.5 text-success" />
                <span class="tabular-nums">Every source is complete in this window · {{ completeSources }} {{ completeSources === 1 ? 'source' : 'sources' }}</span>
                <UButton label="Show"
                         size="xs"
                         color="neutral"
                         variant="link"
                         @click="showComplete = true" />
            </div>
        </template>
        <UEmpty v-else
                :icon="icons.window"
                :title="NO_PARTITIONS"
                variant="naked"
                size="sm" />
        <ExecutionsRunModal v-if="runTarget"
                            v-model:open="runOpen"
                            :target="runTarget"
                            :initial-range="{ start: date, end: date }" />
    </div>
</template>
