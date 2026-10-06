<script setup lang="ts">
const overviewStore = useOverviewStore()
const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const { overview, error } = storeToRefs(overviewStore)

function refresh() {
    overviewStore.fetchOverview()
    overviewStore.fetchCoverage()
}

onMounted(async () => {
    await Promise.all([
        overviewStore.fetchOverview(),
        overviewStore.fetchCoverage(),
        componentsStore.fetchAll(['job', 'source', 'asset', 'connection']),
        catalogStore.loaded ? Promise.resolve() : catalogStore.fetchCatalog(),
    ])
})

onUnmounted(() => overviewStore.$reset())
</script>

<template>
    <UDashboardPanel id="overview">
        <template #header>
            <AppNavbar title="Overview" />
        </template>
        <template #body>
            <UAlert v-if="error"
                    color="error"
                    icon="i-lucide-triangle-alert"
                    title="Couldn't load the overview"
                    :description="errorDetail(error) ?? GENERIC_ERROR"
                    :actions="[{
                        label: 'Retry',
                        icon: 'i-lucide-refresh-cw',
                        color: 'neutral',
                        variant: 'outline',
                        onClick: refresh,
                    }]" />

            <!-- Every section is drawn from the first frame and fills in as its data arrives; only a failed overview drops the ones it feeds. -->
            <template v-if="overview || !error">
                <OverviewHealthStrip :overview="overview" />
                <OverviewAttentionList :items="overview?.attention ?? null"
                                       :generated-at="overview?.generated_at ?? null" />
            </template>
            <OverviewTimelineSection :upcoming="overview?.upcoming ?? []" />
            <OverviewCoverageCalendar />
            <template v-if="overview || !error">
                <div class="grid grid-cols-1 gap-4 sm:gap-6 lg:grid-cols-2">
                    <OverviewUpcomingList :items="overview?.upcoming ?? null" />
                    <OverviewRecentList :runs="overview?.recent ?? null" />
                </div>
                <OverviewComponentsInventory :rows="overview?.components ?? null" />
            </template>
        </template>
    </UDashboardPanel>
</template>
