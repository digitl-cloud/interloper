<script setup lang="ts">
definePageMeta({ title: 'Executions', fullBleed: true })

const route = useRoute()
const router = useRouter()

const items = [
    { label: 'Runs', value: 'runs', icon: 'i-lucide-activity' },
    { label: 'Backfills', value: 'backfills', icon: 'i-lucide-history' },
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
        <PageTabs v-model="activeTab"
                  :items="items">
            <template #runs>
                <ExecutionsRunsTable />
            </template>
            <template #backfills>
                <ExecutionsBackfillsTable />
            </template>
        </PageTabs>
    </div>
</template>
