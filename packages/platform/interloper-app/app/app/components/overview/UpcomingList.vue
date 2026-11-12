<script setup lang="ts">
import { h } from 'vue'
import { UIcon } from '#components'
import type { UpcomingRun } from '~/types/overview'

const organisationStore = useOrganisationStore()
const overviewStore = useOverviewStore()
const { error } = storeToRefs(overviewStore)
const appConfig = useAppConfig()

const props = defineProps<{ items: UpcomingRun[] | null }>()
const LIMIT = 4

const userStore = useUserStore()
const timezone = computed(() => userStore.user?.timezone ?? Intl.DateTimeFormat().resolvedOptions().timeZone)

const rows = computed(() => (props.items ?? []).slice(0, LIMIT).map((item) => {
    const at = new Date(item.next_run_at)
    return {
        ...item,
        rel: relativeTime(at),
        at: formatDayTime(at),
    }
}))

type Row = (typeof rows.value)[number]

const columns = withSkeletons<Row>([
    {
        id: 'job',
        header: 'Job',
        cell: ({ row }) => h('span', { class: 'flex min-w-0 items-center gap-2' }, [
            h(UIcon, { name: appConfig.icons.job, class: 'size-4 shrink-0 text-dimmed' }),
            h('span', { class: 'truncate font-medium text-highlighted' }, row.original.job_name),
        ]),
    },
    {
        id: 'next',
        header: 'Next run',
        cell: ({ row }) => h('span', { class: 'font-semibold tabular-nums text-highlighted' }, row.original.rel),
    },
    {
        id: 'at',
        header: 'Scheduled',
        meta: { class: { th: 'text-right', td: 'text-right text-xs tabular-nums text-dimmed' } },
        cell: ({ row }) => row.original.at,
    },
])
</script>

<template>
    <UCard>
        <template #header>
            <CardHeader title="Coming up"
                        :description="timezone">
                <UButton label="All jobs"
                         :to="organisationStore.path(kindPath('job'))"
                         color="neutral"
                         variant="outline"
                         size="sm" />
            </CardHeader>
        </template>
        <LoadError v-if="!items && error"
                   title="Couldn't load upcoming runs"
                   :error="error"
                   @retry="overviewStore.fetchOverview()" />
        <UTable v-else
                :data="items ? rows : skeletonRows(LIMIT)"
                :columns="columns"
                :ui="{ tr: 'cursor-pointer' }"
                @select="(_e: Event, row: any) => isSkeletonRow(row.original) || navigateTo({ path: organisationStore.path(kindPath('job')), query: { q: row.original.job_name } })">
            <template #empty>
                <UEmpty :icon="appConfig.icons.job"
                        title="Nothing scheduled"
                        description="Enabled jobs list their next firing here."
                        variant="naked"
                        size="sm" />
            </template>
        </UTable>
    </UCard>
</template>
