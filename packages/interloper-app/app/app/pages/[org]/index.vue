<script setup lang="ts">
const overviewStore = useOverviewStore()
const componentsStore = useComponentsStore()
const organisationStore = useOrganisationStore()
const { overview, error } = storeToRefs(overviewStore)
const editor = useCanEdit()

/** A new organisation has nothing to sum up, so the page points at its first steps instead of a wall of zeros. */
const blank = computed(() => !!overview.value && overview.value.components.every(row => !row.total))

function refresh() {
    overviewStore.fetchOverview()
    overviewStore.fetchCoverage()
}

onMounted(async () => {
    await Promise.all([
        overviewStore.fetchOverview(),
        overviewStore.fetchCoverage(),
        componentsStore.fetchAll(['job', 'source', 'asset', 'connection']),
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

            <div v-if="blank"
                 class="mx-auto w-full max-w-[1040px]">
                <EmptyState icon="i-lucide-layout-dashboard"
                            :title="`Nothing in ${organisationStore.organisation?.name ?? 'this organisation'} yet`"
                            description="The overview sums up your runs, schedule and partition coverage. Add a source to pull your first data, then a job to keep it fresh.">
                    <div v-if="editor"
                         class="mt-5 flex justify-center gap-2">
                        <UButton icon="i-lucide-plus"
                                 label="Add a source"
                                 :to="organisationStore.path(kindPath('source'))" />
                        <UButton icon="i-lucide-calendar-plus"
                                 label="Create a job"
                                 color="neutral"
                                 variant="outline"
                                 :to="organisationStore.path(kindPath('job'))" />
                    </div>
                </EmptyState>
            </div>

            <!-- Every section is drawn from the first frame and fills in as its data arrives; only a failed overview drops the ones it feeds. -->
            <template v-else>
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
        </template>
    </UDashboardPanel>
</template>
