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
        <UTabs :items="items"
               variant="link"
               :model-value="activeTab"
               class="overflow-auto"
               :ui="{ list: 'px-4 pt-4' }"
               @update:model-value="activeTab = $event as string">
            <template #runs>
                <ExecutionsRunsTable />
            </template>
            <template #backfills>
                <ExecutionsBackfillsTable />
            </template>
        </UTabs>
</template>
