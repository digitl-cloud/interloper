<script setup lang="ts">
definePageMeta({ title: 'Overview' })

const overviewStore = useOverviewStore()
const componentsStore = useComponentsStore()
const catalogStore = useCatalogStore()
const { overview, loading, error } = storeToRefs(overviewStore)

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
    <div class="mx-auto flex w-full max-w-[1360px] flex-col gap-7">
        <NavActions>
            <UButton icon="i-lucide-refresh-cw"
                     color="neutral"
                     variant="outline"
                     size="sm"
                     :loading="loading"
                     aria-label="Refresh"
                     @click="refresh" />
        </NavActions>

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

        <OverviewHealthStrip v-if="overview"
                             :overview="overview" />
        <OverviewAttentionList v-if="overview"
                               :items="overview.attention"
                               :generated-at="overview.generated_at" />
        <OverviewTimelineSection :upcoming="overview?.upcoming ?? []" />
        <OverviewCoverageCalendar />
        <div v-if="overview"
             class="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <OverviewUpcomingList :items="overview.upcoming" />
            <OverviewRecentList :runs="overview.recent" />
        </div>
        <OverviewComponentsInventory v-if="overview"
                                     :rows="overview.components" />
    </div>
</template>
