<script setup lang="ts">
import type { AccordionItem, ListboxItem } from '@nuxt/ui'
import type { TimelineRow } from '~/types/timeline'
import type { RunAttempt } from '~/composables/runAttempts'
import type { RunStatusBucket } from '~/composables/runStats'

const props = defineProps<{
    /** This attempt's assets, unfiltered: the rail is where the status filter is chosen. */
    rows: TimelineRow[]
    buckets: RunStatusBucket[]
    /** The stack's attempts, oldest first; the section shows only once there is more than one. */
    attempts: RunAttempt[]
    currentRunId: string
}>()

const statusFilter = defineModel<string | null>('statusFilter', { default: null })
/** Asset drilled into, shared with the timeline and the graph. */
const selected = defineModel<string | null>('selected', { default: null })
/** Asset under the pointer or the keyboard, mirrored in the timeline. */
const hovered = defineModel<string | null>('hovered', { default: null })

const openSections = ref(['attempts', 'assets'])

// The attempt list is short (bounded by the retry budget), so it keeps its
// height and the asset list is the one that scrolls when the rail runs out.
const sections = computed<AccordionItem[]>(() => [
    ...(props.attempts.length > 1 ? [{ label: 'Attempts', value: 'attempts', slot: 'attempts', count: props.attempts.length, ui: { item: 'shrink-0 max-h-1/2' } }] : []),
    { label: 'Assets', value: 'assets', slot: 'assets', count: props.rows.length },
])

const STATUS_PREFIX = 'status:'

// One listbox group per status: its header is an item too, so it can be picked
// (it toggles the status filter instead of becoming the selection). Reka
// memoises each option until its highlight or selection changes, so items carry
// only identity; the slot reads everything that moves (filter, counts,
// durations) reactively.
const assetGroups = computed<ListboxItem[][]>(() => props.buckets
    .map(bucket => [
        {
            value: `${STATUS_PREFIX}${bucket.key}`,
            label: bucket.label,
            bucket: bucket.key,
            header: true,
            class: 'mt-1',
            onSelect: (event: Event) => {
                event.preventDefault()
                statusFilter.value = statusFilter.value === bucket.key ? null : bucket.key
            },
        },
        ...props.rows
            .filter(row => bucketKeyForStatus(row.status ?? 'pending') === bucket.key)
            .map(row => ({ value: row.id ?? row.name, label: row.name, icon: row.icon, bucket: bucket.key, disabled: !row.id, class: 'ps-5' })),
    ])
    .filter(group => group.length > 1))

const bucketsByKey = computed(() => new Map(props.buckets.map(bucket => [bucket.key, bucket])))

const durations = computed(() => new Map(props.rows.map((row) => {
    const bar = row.bars[0]
    return [row.id ?? row.name, bar ? formatElapsed(new Date(bar.start), bar.end ? new Date(bar.end) : null) : '—']
})))

function onHighlight(payload: { value: unknown } | undefined) {
    const value = payload?.value
    hovered.value = typeof value === 'string' && !value.startsWith(STATUS_PREFIX) ? value : null
}

const widest = computed(() => Math.max(1, ...props.attempts.map(attempt => attempt.stats.total)))

function outcome(attempt: RunAttempt): string {
    const started = attempt.run.started_at ? new Date(attempt.run.started_at).toLocaleTimeString('en-US', { hour12: false }) : 'not started'
    return [...outcomeSummary(attempt.stats), started].join(' · ')
}
</script>

<template>
    <UAccordion v-model="openSections"
                type="multiple"
                :items="sections"
                :ui="{
                    root: 'flex w-full flex-col gap-3',
                    item: 'overflow-hidden rounded-lg border-0 bg-muted ring ring-default',
                    trigger: 'px-4 py-3 text-sm font-semibold',
                    content: 'flex flex-col',
                }">
        <template #trailing="{ item, ui }">
            <span class="ms-auto text-xs font-normal text-muted tabular-nums">{{ item.count }}</span>
            <UIcon name="i-lucide-chevron-down"
                   :class="ui.trailingIcon({ class: 'ms-0 size-3.5 text-dimmed' })" />
        </template>

        <template #assets>
            <UListbox :model-value="selected ?? undefined"
                      :items="assetGroups"
                      value-key="value"
                      size="sm"
                      :ui="{
                          root: 'rounded-none bg-transparent ring-0',
                          content: 'max-h-none divide-y-0 pb-2',
                          group: 'px-2 pb-0 pt-1',
                          item: 'items-center data-[state=checked]:text-primary data-[state=checked]:before:bg-primary/10',
                      }"
                      @update:model-value="value => selected = (value as string | undefined) ?? null"
                      @highlight="onHighlight"
                      @leave="hovered = null">
                <template #item="{ item, ui }">
                    <template v-if="item.header">
                        <span class="m-1 size-[7px] shrink-0 rounded-full"
                              :class="bucketsByKey.get(item.bucket)?.colorClass" />
                        <span class="flex-1 truncate text-xs font-semibold uppercase tracking-[.05em]"
                              :class="statusFilter === item.bucket ? 'text-primary' : 'text-dimmed'">{{ item.label }}</span>
                        <span class="text-xs font-semibold tabular-nums"
                              :class="statusFilter === item.bucket ? 'text-primary' : 'text-dimmed'">{{ bucketsByKey.get(item.bucket)?.count }}</span>
                    </template>
                    <span v-else
                          class="flex min-w-0 flex-1 items-center gap-1.5 transition-opacity"
                          :class="statusFilter && statusFilter !== item.bucket ? 'opacity-45' : ''">
                        <UIcon :name="item.icon"
                               :class="ui.itemLeadingIcon({ class: 'size-3.5' })" />
                        <span class="min-w-0 flex-1 truncate font-mono">{{ item.label }}</span>
                        <span class="shrink-0 font-mono text-xs text-dimmed">{{ durations.get(item.value) }}</span>
                    </span>
                </template>
            </UListbox>
        </template>

        <template #attempts>
            <div>
                <UButton v-for="attempt in attempts"
                         :key="attempt.run.id"
                         :to="`/executions/runs/${attempt.run.id}`"
                         block
                         color="neutral"
                         variant="ghost"
                         :active="attempt.run.id === currentRunId"
                         active-color="primary"
                         active-variant="soft"
                         class="flex-col items-stretch gap-2 rounded-none px-4 py-2 text-left">
                    <span class="flex items-center gap-2">
                        <span class="size-2 shrink-0 rounded-full"
                              :class="runStatusDotClass(attempt.run.status)" />
                        <span class="text-sm font-semibold">Attempt {{ attempt.run.attempt }}</span>
                        <span class="font-mono text-xs font-normal text-dimmed">{{ attempt.run.id.substring(0, 8) }}</span>
                        <span class="ms-auto text-xs font-normal text-muted tabular-nums">{{ attempt.stats.duration ?? '—' }}</span>
                    </span>
                    <UProgressGroup :items="progressSegments(attempt.stats)"
                                    :max="widest"
                                    size="sm"
                                    :ui="{ base: 'h-1.5' }" />
                    <span class="truncate text-xs font-normal text-dimmed">{{ outcome(attempt) }}</span>
                </UButton>
            </div>
        </template>
    </UAccordion>
</template>
