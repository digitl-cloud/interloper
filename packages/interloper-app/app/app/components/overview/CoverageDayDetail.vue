<script setup lang="ts">
import { h } from 'vue'
import type { TableColumn } from '@nuxt/ui'
import { UButton, UIcon, ULink } from '#components'
import type { ComponentRecord } from '~/types/component'
import type { Coverage, CoverageDay } from '~/types/overview'

const props = defineProps<{
    date: string
    coverage: Coverage
    sourceFilter: string
}>()

/** Problem rows shown before the rest folds behind "Show all". */
const PROBLEM_CAP = 5

const componentsStore = useComponentsStore()
const editor = useCanEdit()
const expanded = ref(false)
watch(() => props.date, () => { expanded.value = false })

const rows = computed(() => props.coverage.sources
    .filter(s => props.sourceFilter === 'all' || s.id === props.sourceFilter)
    .flatMap((s) => {
        const d = sourceDay(s, props.date)
        if (!d) return []
        return [{
            ...d,
            name: s.name,
            kind: s.kind,
            okPct: Math.round((100 * d.covered) / Math.max(1, d.expected)),
            failPct: Math.round((100 * d.failed) / Math.max(1, d.expected)),
            gap: d.covered + d.failed < d.expected,
            missing: d.expected - d.covered - d.failed,
        }]
    }))

const problems = computed(() => rows.value
    .filter(r => r.failed || r.gap)
    .sort((a, b) => b.failed - a.failed || b.missing - a.missing || a.name.localeCompare(b.name)))
const complete = computed(() => rows.value
    .filter(r => !r.failed && !r.gap)
    .sort((a, b) => a.name.localeCompare(b.name)))

const visible = computed(() => {
    if (expanded.value) return [...problems.value, ...complete.value]
    return problems.value.length ? problems.value.slice(0, PROBLEM_CAP) : complete.value.slice(0, PROBLEM_CAP)
})
const hiddenProblems = computed(() => Math.max(0, problems.value.length - PROBLEM_CAP))
const hiddenComplete = computed(() => problems.value.length
    ? complete.value.length
    : Math.max(0, complete.value.length - PROBLEM_CAP))
const foldable = computed(() => hiddenProblems.value > 0 || hiddenComplete.value > 0)
const foldedNote = computed(() => {
    const parts: string[] = []
    if (hiddenProblems.value) parts.push(`${hiddenProblems.value} more with gaps`)
    if (hiddenComplete.value) parts.push(`${hiddenComplete.value} complete`)
    return parts.join(' · ')
})

const summary = computed(() => {
    const expected = rows.value.reduce((n, r) => n + r.expected, 0)
    const covered = rows.value.reduce((n, r) => n + r.covered, 0)
    const failed = rows.value.reduce((n, r) => n + r.failed, 0)
    if (!rows.value.length) return 'Nothing expected on this day'
    const parts = [`${covered} of ${expected} partitions covered`]
    if (failed) parts.push(`${failed} failed`)
    if (covered + failed < expected) parts.push(`${expected - covered - failed} missing`)
    return parts.join(' · ')
})

const label = computed(() => new Date(`${props.date}T00:00:00Z`).toLocaleDateString(undefined, {
    weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
}))

type Row = (typeof rows.value)[number]

const columns: TableColumn<Row>[] = [
    {
        id: 'source',
        header: 'Source',
        cell: ({ row }) => h('span', { class: 'flex min-w-0 items-center gap-1.5' }, [
            h(UIcon, { name: row.original.kind === 'asset' ? 'i-lucide-box' : 'i-lucide-plug', class: 'size-3.5 shrink-0 text-dimmed' }),
            h('span', { class: 'truncate font-mono text-xs text-highlighted' }, row.original.name),
        ]),
    },
    {
        id: 'coverage',
        header: 'Coverage',
        meta: { class: { td: 'w-full' } },
        cell: ({ row }) => h('div', { class: 'flex h-2 overflow-hidden rounded-full bg-accented' }, [
            h('div', { class: 'bg-success', style: { width: `${row.original.okPct}%` } }),
            h('div', { class: 'bg-error', style: { width: `${row.original.failPct}%` } }),
        ]),
    },
    {
        id: 'partitions',
        header: 'Partitions',
        meta: { class: { th: 'text-right', td: 'text-right text-xs tabular-nums text-muted' } },
        cell: ({ row }) => `${row.original.covered} / ${row.original.expected}`,
    },
    {
        id: 'action',
        header: '',
        meta: { class: { td: 'text-right' } },
        cell: ({ row }) => {
            const r = row.original
            if (editor.value && r.gap && !r.failed)
                return h(UButton, { icon: 'i-lucide-play', label: 'Run', size: 'xs', color: 'neutral', variant: 'outline', onClick: () => run(r) })
            if (r.failed && r.failed_run_id)
                return h(ULink, { to: `/executions/runs/${r.failed_run_id}`, class: 'inline-flex items-center gap-1 text-xs text-error hover:underline' },
                    () => ['Open run', h(UIcon, { name: 'i-lucide-arrow-right', class: 'size-3' })])
            if (!r.gap && !r.failed)
                return h('span', { class: 'inline-flex items-center gap-1 text-xs text-success' },
                    [h(UIcon, { name: 'i-lucide-check', class: 'size-3' }), 'Complete'])
            return null
        },
    },
]

const runTarget = ref<ComponentRecord | null>(null)
const runOpen = ref(false)
function run(row: CoverageDay) {
    const target = componentsStore.byId(row.source_id)
    if (!target) return
    runTarget.value = target
    runOpen.value = true
}
</script>

<template>
    <div class="mt-5">
        <div class="mb-2.5 flex flex-wrap items-baseline gap-2.5">
            <span class="text-sm font-semibold text-highlighted">{{ label }}</span>
            <span class="text-xs text-muted">{{ summary }}</span>
        </div>
        <UTable v-if="rows.length"
                :data="visible"
                :columns="columns" />
        <div v-if="rows.length && foldable"
             class="mt-2.5 flex items-center gap-2 text-xs text-muted">
            <span v-if="!expanded">{{ foldedNote }}</span>
            <UButton :label="expanded ? 'Show less' : 'Show all'"
                     size="xs"
                     color="neutral"
                     variant="link"
                     class="p-0"
                     @click="expanded = !expanded" />
        </div>
        <ExecutionsRunModal v-if="runTarget"
                            v-model:open="runOpen"
                            :target="runTarget"
                            :initial-range="{ start: date, end: date }" />
    </div>
</template>
