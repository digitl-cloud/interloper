<script setup lang="ts">
import type { AttentionItem } from '~/types/overview'

defineProps<{
    items: AttentionItem[] | null
    generatedAt: string | null
}>()

const SKELETON_ROWS = 3

const editor = useCanEdit()

const KIND_META: Record<AttentionItem['kind'], { label: string, icon: string, rowIcon: string }> = {
    error_group: { label: 'Error group', icon: 'i-lucide-circle-alert', rowIcon: 'i-lucide-x' },
    connection: { label: 'Connection', icon: 'i-lucide-key-round', rowIcon: 'i-lucide-key-round' },
    run_stack: { label: 'Run stack', icon: 'i-lucide-activity', rowIcon: 'i-lucide-repeat' },
    drift: { label: 'Catalog drift', icon: 'i-lucide-library', rowIcon: 'i-lucide-circle-help' },
    overdue: { label: 'Overdue job', icon: 'i-lucide-calendar-clock', rowIcon: 'i-lucide-clock' },
}

function openTarget(item: AttentionItem): { label: string, to: string } {
    switch (item.kind) {
        case 'error_group':
        case 'run_stack':
            return { label: 'Open run', to: `/executions/runs/${item.run_id}` }
        case 'connection':
            return { label: 'Open connection', to: kindPath('connection') }
        case 'drift':
            return { label: 'Open collection', to: '/collection' }
        case 'overdue':
            return { label: 'Open job', to: kindPath('job') }
    }
}

function fix(item: AttentionItem): { label: string, icon: string, to: string } | null {
    if (!editor.value) return null
    if (item.kind === 'connection' && item.component_id) {
        return { label: 'Reconnect', icon: 'i-lucide-plug-zap', to: `${kindPath('connection')}?edit=${item.component_id}` }
    }
    return null
}

function when(item: AttentionItem): string {
    if (!item.since) return ''
    if (item.kind === 'overdue') return `due ${formatDate(item.since)}`
    if (item.kind === 'error_group') return `last seen ${timeSince(new Date(item.since))} ago`
    return `${timeSince(new Date(item.since))} ago`
}
</script>

<template>
    <UCard :ui="{ body: 'p-0 sm:p-0' }">
        <template #header>
            <CardHeader title="Needs attention"
                        :description="items?.length ? `${items.length} item${items.length === 1 ? '' : 's'}` : undefined"
                        :loading="!items" />
        </template>
        <div v-if="!items"
             class="divide-y divide-default border-t border-default">
            <div v-for="i in SKELETON_ROWS"
                 :key="i"
                 class="flex items-center gap-3.5 px-4 py-3 sm:px-6">
                <USkeleton class="size-8 shrink-0 rounded-full" />
                <div class="flex min-w-0 flex-1 flex-col gap-1.5 py-0.5">
                    <USkeleton class="h-4 w-2/3" />
                    <USkeleton class="h-3.5 w-1/3" />
                </div>
                <USkeleton class="h-8 w-24 shrink-0" />
            </div>
        </div>
        <div v-else-if="items.length"
             class="divide-y divide-default border-t border-default">
            <div v-for="item in items"
                 :key="`${item.kind}:${item.run_id ?? item.component_id ?? ''}:${item.title}`"
                 class="flex items-center gap-3.5 px-4 py-3 transition-colors hover:bg-elevated/50 sm:px-6">
                <span class="inline-flex size-8 shrink-0 items-center justify-center rounded-full"
                      :class="item.severity === 'error' ? 'bg-error/10 text-error' : 'bg-warning/15 text-warning'">
                    <UIcon :name="KIND_META[item.kind].rowIcon"
                           class="size-4" />
                </span>
                <div class="flex min-w-0 flex-1 flex-col gap-0.5">
                    <div class="text-sm font-medium leading-snug text-highlighted">{{ item.title }}</div>
                    <div class="flex flex-wrap items-center gap-2 text-xs text-dimmed">
                        <span class="inline-flex items-center gap-1.5 text-muted">
                            <UIcon :name="KIND_META[item.kind].icon"
                                   class="size-3.5" />{{ KIND_META[item.kind].label }}
                        </span>
                        <template v-if="item.target">
                            <span>·</span>
                            <span class="font-mono text-xs">{{ item.target }}</span>
                        </template>
                        <template v-if="when(item)">
                            <span>·</span>
                            <span>{{ when(item) }}</span>
                        </template>
                    </div>
                </div>
                <div class="flex shrink-0 items-center gap-2">
                    <UButton v-if="fix(item)"
                             :icon="fix(item)!.icon"
                             :label="fix(item)!.label"
                             :to="fix(item)!.to"
                             size="sm" />
                    <UButton :label="openTarget(item).label"
                             :to="openTarget(item).to"
                             size="sm"
                             color="neutral"
                             variant="outline" />
                </div>
            </div>
        </div>
        <div v-else
             class="flex items-center gap-3.5 p-4 sm:p-6">
            <span class="inline-flex size-9 shrink-0 items-center justify-center rounded-full bg-success/10 text-success">
                <UIcon name="i-lucide-check"
                       class="size-4" />
            </span>
            <div class="flex flex-col gap-0.5">
                <div class="text-sm font-semibold text-highlighted">All clear</div>
                <div class="text-sm text-muted">No failures, drift or overdue jobs. Last checked {{ formatClockTime(new Date(generatedAt ?? Date.now())) }}.</div>
            </div>
        </div>
    </UCard>
</template>
