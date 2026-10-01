<script setup lang="ts">
import type { Run } from '~/types/run'

const props = defineProps<{ runs: Run[] }>()

const DOT: Record<ReturnType<typeof statusPillColor>, string> = {
    success: 'bg-success',
    error: 'bg-error',
    primary: 'bg-primary',
    warning: 'bg-warning',
    neutral: 'bg-accented',
}

const rows = computed(() => props.runs.map((run) => {
    const value = run.completed_at ?? run.started_at ?? run.created_at
    const at = value ? new Date(value) : null
    return {
        run,
        color: statusPillColor(run.status),
        rel: at ? relativeTime(at) : '',
        at: at ? `${formatShortDay(at)} ${formatClockTime(at)}` : '',
    }
}))
</script>

<template>
    <OverviewSection title="Just happened"
                     link-label="All executions"
                     link-to="/executions/runs">
        <div class="overflow-hidden rounded-lg border border-default divide-y divide-default">
            <NuxtLink v-for="row in rows"
                      :key="row.run.id"
                      :to="`/executions/runs/${row.run.id}`"
                      class="flex items-center gap-3 px-4 py-[11px] text-highlighted transition-colors hover:bg-muted">
                <span class="size-2 shrink-0 rounded-full"
                      :class="DOT[row.color]" />
                <span class="min-w-0 flex-1 truncate font-mono text-[12.5px]">{{ row.run.component_name ?? row.run.component_key ?? 'Deleted target' }}</span>
                <StatusPill :label="row.run.status"
                            :color="row.color"
                            :dot="false"
                            class="w-16 justify-center capitalize" />
                <span class="whitespace-nowrap text-[13px] font-semibold">{{ row.rel }}</span>
                <span class="w-[132px] shrink-0 whitespace-nowrap text-right text-xs tabular-nums text-dimmed">{{ row.at }}</span>
            </NuxtLink>
            <div v-if="!runs.length"
                 class="px-4 py-5 text-sm text-muted">No runs yet.</div>
        </div>
    </OverviewSection>
</template>
