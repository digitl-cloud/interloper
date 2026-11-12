<script setup lang="ts">
import { h } from 'vue'
import { EntityBadge, StatusBadge } from '#components'
import type { Run } from '~/types/run'

const organisationStore = useOrganisationStore()
const overviewStore = useOverviewStore()
const { error } = storeToRefs(overviewStore)
const appConfig = useAppConfig()

const props = defineProps<{ runs: Run[] | null }>()

/** As many rows as the overview returns, so the loaded table takes the skeleton's place. */
const SKELETON_ROWS = 5

const rows = computed(() => (props.runs ?? []).map((run) => {
    const value = run.completed_at ?? run.started_at ?? run.created_at
    const at = value ? new Date(value) : null
    return {
        run,
        rel: at ? relativeTime(at) : '',
        at: at ? formatDayTime(at) : '',
    }
}))

type Row = (typeof rows.value)[number]

const columns = withSkeletons<Row>([
    {
        id: 'target',
        header: 'Target',
        cell: ({ row }) => h(EntityBadge, { label: targetLabel(row.original.run), icon: targetIcon(row.original.run) }),
    },
    {
        id: 'status',
        header: 'Status',
        cell: ({ row }) => h(StatusBadge, { status: row.original.run.status }),
    },
    {
        id: 'when',
        header: 'When',
        cell: ({ row }) => h('span', { class: 'font-semibold tabular-nums text-highlighted' }, row.original.rel),
    },
    {
        id: 'at',
        header: 'Time',
        meta: { class: { th: 'text-right', td: 'text-right text-xs tabular-nums text-dimmed' } },
        cell: ({ row }) => row.original.at,
    },
])
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Just happened">
                <UButton label="All executions"
                         :to="organisationStore.path('/executions/runs')"
                         color="neutral"
                         variant="outline"
                         size="sm" />
            </CardHeader>
        </template>
        <LoadError v-if="!runs && error"
                   title="Couldn't load recent runs"
                   :error="error"
                   @retry="overviewStore.fetchOverview()" />
        <UTable v-else
                :data="runs ? rows : skeletonRows(SKELETON_ROWS)"
                :columns="columns"
                :ui="{ tr: 'cursor-pointer' }"
                @select="(_e: Event, row: any) => isSkeletonRow(row.original) || navigateTo(organisationStore.path(`/executions/runs/${row.original.run.id}`))">
            <template #empty>
                <UEmpty :icon="appConfig.icons.run"
                        title="No runs yet"
                        description="The latest runs land here as they finish."
                        variant="naked"
                        size="sm" />
            </template>
        </UTable>
    </UCard>
</template>
