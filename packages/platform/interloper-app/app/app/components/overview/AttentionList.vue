<script setup lang="ts">
import type { RouteLocationRaw } from 'vue-router'
import type { AttentionItem } from '~/types/overview'

const organisationStore = useOrganisationStore()
const overviewStore = useOverviewStore()
const { error } = storeToRefs(overviewStore)
const { ui, icons } = useAppConfig()

defineProps<{
    items: AttentionItem[] | null
    generatedAt: string | null
}>()

const SKELETON_ROWS = 3

const editor = useCanEdit()

const KIND_META: Record<AttentionItem['kind'], { label: string, icon: string }> = {
    error_group: { label: 'Error group', icon: ui.icons.error },
    connection: { label: 'Connection', icon: icons.connection },
    run_stack: { label: 'Run stack', icon: ui.icons.reload },
    drift: { label: 'Catalog drift', icon: icons.drift },
    overdue: { label: 'Overdue job', icon: icons.job },
}

/** Where an item opens: the run on its errors, or the list searched down to the component. */
function openTarget(item: AttentionItem): { label: string, to: RouteLocationRaw } {
    switch (item.kind) {
        case 'error_group':
        case 'run_stack':
            return { label: 'Open run', to: { path: organisationStore.path(`/executions/runs/${item.run_id}`), query: { events: 'errors' } } }
        case 'connection':
            return { label: 'Open connection', to: organisationStore.path(kindPath('connection')) }
        case 'drift':
            if (!item.component_kind || item.component_kind === 'asset')
                return { label: 'Open collection', to: { path: organisationStore.path('/collection'), query: { q: item.target ?? undefined } } }
            return { label: `Open ${item.component_kind}`, to: { path: organisationStore.path(kindPath(item.component_kind)), query: { type: item.target ?? undefined } } }
        case 'overdue':
            return { label: 'Open job', to: { path: organisationStore.path(kindPath('job')), query: { q: item.target ?? undefined } } }
    }
}

function fix(item: AttentionItem): { label: string, icon: string, to: string } | null {
    if (!editor.value) return null
    if (item.kind === 'connection' && item.component_id) {
        return { label: 'Reconnect', icon: icons.connection, to: `${organisationStore.path(kindPath('connection'))}?edit=${item.component_id}` }
    }
    return null
}

function when(item: AttentionItem): string {
    if (!item.since) return ''
    if (item.kind === 'overdue') return `Due ${formatDayTime(item.since)}`
    const ago = relativeTime(new Date(item.since))
    return item.kind === 'error_group' ? `Last seen ${ago}` : ago
}
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Needs attention"
                        :description="items?.length ? `${items.length} item${items.length === 1 ? '' : 's'}` : undefined"
                        :loading="!items && !error" />
        </template>
        <LoadError v-if="!items && error"
                   title="Couldn't load what needs attention"
                   :error="error"
                   @retry="overviewStore.fetchOverview()" />
        <div v-else-if="!items"
             class="divide-y divide-default">
            <div v-for="i in SKELETON_ROWS"
                 :key="i"
                 class="flex items-center gap-3 py-3">
                <USkeleton class="size-5 shrink-0 rounded-full" />
                <div class="flex min-w-0 flex-1 flex-col gap-1.5">
                    <USkeleton class="h-4 w-2/3" />
                    <USkeleton class="h-3.5 w-1/3" />
                </div>
                <USkeleton class="h-8 w-24 shrink-0" />
            </div>
        </div>
        <div v-else-if="items.length"
             class="divide-y divide-default">
            <div v-for="item in items"
                 :key="`${item.kind}:${item.run_id ?? item.component_id ?? ''}:${item.title}`"
                 class="flex items-center gap-3 py-3">
                <UIcon :name="KIND_META[item.kind].icon"
                       class="size-5 shrink-0"
                       :class="item.severity === 'error' ? 'text-error' : 'text-warning'" />
                <div class="flex min-w-0 flex-1 flex-col gap-0.5">
                    <div class="text-sm font-medium text-highlighted">{{ item.title }}</div>
                    <div class="flex flex-wrap items-center gap-2 text-xs text-dimmed">
                        <span class="text-muted">{{ KIND_META[item.kind].label }}</span>
                        <template v-if="item.target">
                            <span>·</span>
                            <span>{{ item.target }}</span>
                        </template>
                        <template v-if="when(item)">
                            <span>·</span>
                            <span class="tabular-nums">{{ when(item) }}</span>
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
        <UEmpty v-else
                :icon="ui.icons.success"
                title="All clear"
                :description="`No failures, drift or overdue jobs. Last checked ${formatClockTime(new Date(generatedAt ?? Date.now()))}.`"
                variant="naked"
                size="sm" />
    </UCard>
</template>
