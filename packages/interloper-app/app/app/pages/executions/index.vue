<script setup lang="ts">
definePageMeta({ title: 'Executions' })

const route = useRoute()
const router = useRouter()

const items = [
    { label: 'Runs', value: 'runs', slot: 'runs' },
    { label: 'Backfills', value: 'backfills', slot: 'backfills' },
]

const activeTab = computed({
    get: () => (route.query.tab as string) || 'runs',
    set: (value: string) => router.push({ query: { ...route.query, tab: value } }),
})

onMounted(() => {
    if (!route.query.tab) {
        router.replace({ query: { tab: 'runs' } })
    }
})
</script>

<template>
    <div class="flex flex-col flex-1 min-h-0">
        <UTabs :items="items"
               variant="link"
               :model-value="activeTab"
               :ui="{ list: 'mb-4' }"
               @update:model-value="activeTab = $event as string">
            <template #runs>
                <ExecutionsRunsTable />
            </template>
            <template #backfills>
                <ExecutionsBackfillsTable />
            </template>
        </UTabs>
    </div>
</template>
