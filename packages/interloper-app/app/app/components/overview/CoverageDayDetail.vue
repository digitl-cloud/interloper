<script setup lang="ts">
import type { ComponentRecord } from '~/types/component'
import type { Coverage, CoverageDay } from '~/types/overview'

const props = defineProps<{
    date: string
    coverage: Coverage
    jobFilter: string
}>()

const componentsStore = useComponentsStore()
const editor = useCanEdit()

const rows = computed(() => props.coverage.days
    .filter(d => d.date === props.date && (props.jobFilter === 'all' || d.job_id === props.jobFilter))
    .map(d => ({
        ...d,
        name: props.coverage.jobs.find(j => j.id === d.job_id)?.name ?? d.job_id.slice(0, 8),
        okPct: Math.round((100 * d.covered) / Math.max(1, d.expected)),
        failPct: Math.round((100 * d.failed) / Math.max(1, d.expected)),
        gap: d.covered + d.failed < d.expected,
    })))

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

const backfillJob = ref<ComponentRecord | null>(null)
const backfillOpen = ref(false)
function backfill(row: CoverageDay) {
    const job = componentsStore.byId(row.job_id)
    if (!job) return
    backfillJob.value = job
    backfillOpen.value = true
}
</script>

<template>
    <div class="border-t border-default bg-muted px-5 pb-4 pt-3.5">
        <div class="mb-2.5 flex flex-wrap items-baseline gap-2.5">
            <span class="text-[13.5px] font-semibold text-highlighted">{{ label }}</span>
            <span class="text-[12.5px] text-muted">{{ summary }}</span>
        </div>
        <div v-if="rows.length"
             class="grid grid-cols-[minmax(160px,220px)_minmax(0,1fr)_72px_auto] items-center gap-x-4 gap-y-2">
            <template v-for="row in rows"
                      :key="row.job_id">
                <span class="truncate font-mono text-xs text-highlighted">{{ row.name }}</span>
                <div class="flex h-2 overflow-hidden rounded-full bg-accented">
                    <div class="bg-success"
                         :style="{ width: `${row.okPct}%` }" />
                    <div class="bg-error"
                         :style="{ width: `${row.failPct}%` }" />
                </div>
                <span class="whitespace-nowrap text-xs tabular-nums text-muted">{{ row.covered }} / {{ row.expected }}</span>
                <div class="flex min-w-[92px] justify-end">
                    <UButton v-if="editor && row.gap && !row.failed"
                             icon="i-lucide-history"
                             label="Backfill"
                             size="xs"
                             color="neutral"
                             variant="outline"
                             @click="backfill(row)" />
                    <ULink v-else-if="row.failed && row.failed_run_id"
                           :to="`/executions/runs/${row.failed_run_id}`"
                           class="inline-flex items-center gap-1 text-xs text-error hover:underline">Open run<UIcon name="i-lucide-arrow-right"
                                                                                                                       class="size-3" /></ULink>
                    <span v-else-if="!row.gap && !row.failed"
                          class="inline-flex items-center gap-1 text-xs text-success"><UIcon name="i-lucide-check"
                                                                                               class="size-3" />Complete</span>
                </div>
            </template>
        </div>
        <ExecutionsRunModal v-if="backfillJob"
                            v-model:open="backfillOpen"
                            :target="backfillJob"
                            :initial-range="{ start: date, end: date }" />
    </div>
</template>
