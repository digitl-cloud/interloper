<script setup lang="ts">
const overviewStore = useOverviewStore()
const componentsStore = useComponentsStore()
const organisationStore = useOrganisationStore()
const { overview } = storeToRefs(overviewStore)
const editor = useCanEdit()
const appConfig = useAppConfig()

/** A new organisation has nothing to sum up, so the page points at its first steps instead of a wall of zeros. */
const blank = computed(() => !!overview.value && overview.value.components.every(row => !row.total))

const firstSteps = computed(() => editor.value
    ? [
            { label: 'New source', icon: appConfig.ui.icons.plus, to: { path: organisationStore.path(kindPath('source')), query: { new: '1' } } },
            { label: 'New job', icon: appConfig.ui.icons.plus, color: 'neutral' as const, variant: 'outline' as const, to: { path: organisationStore.path(kindPath('job')), query: { new: '1' } } },
        ]
    : undefined)

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
            <UEmpty v-if="blank"
                    :icon="appConfig.icons.overview"
                    :title="`Nothing in ${organisationStore.organisation?.name ?? 'this organisation'} yet`"
                    description="The overview sums up your runs, schedule and partition coverage. Add a source to pull your first data, then a job to keep it fresh."
                    :actions="firstSteps" />

            <!-- Every section is drawn from the first frame and fills in as its data arrives; each reports its own failed load. -->
            <template v-else>
                <OverviewHealthStrip :overview="overview" />
                <OverviewAttentionList :items="overview?.attention ?? null"
                                       :generated-at="overview?.generated_at ?? null" />
                <OverviewTimelineSection :upcoming="overview?.upcoming ?? []" />
                <OverviewCoverageCalendar />
                <div class="grid grid-cols-1 gap-4 sm:gap-6 lg:grid-cols-2">
                    <OverviewUpcomingList :items="overview?.upcoming ?? null" />
                    <OverviewRecentList :runs="overview?.recent ?? null" />
                </div>
                <OverviewComponentsInventory :rows="overview?.components ?? null" />
            </template>
        </template>
    </UDashboardPanel>
</template>
