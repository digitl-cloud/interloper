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
    <UDashboardPanel id="overview"
                     class="page-wide"
                     :ui="{ body: 'gap-8 sm:gap-8' }">
        <template #header>
            <AppNavbar title="Overview" />
        </template>
        <template #body>
            <UEmpty v-if="blank"
                    :icon="appConfig.icons.overview"
                    :title="`Nothing in ${organisationStore.organisation?.name ?? 'this organisation'} yet`"
                    description="The overview sums up your runs, schedule and partition coverage. Add a source to pull your first data, then a job to keep it fresh."
                    :actions="firstSteps" />

            <!-- Every card is drawn from the first frame and fills in as its data arrives; each reports its own failed load.
                 The cards come in sections, each under a plain label. -->
            <template v-else>
                <section class="flex flex-col gap-4">
                    <UPageCard title="Health"
                               variant="naked"
                               orientation="horizontal" />
                    <OverviewHealthStrip :overview="overview" />
                    <OverviewAttentionList :items="overview?.attention ?? null"
                                           :generated-at="overview?.generated_at ?? null" />
                </section>
                <section class="flex flex-col gap-4">
                    <UPageCard title="Activity"
                               variant="naked"
                               orientation="horizontal">
                        <UButton label="Open timeline"
                                 :to="{ path: organisationStore.path('/executions/runs'), query: { view: 'timeline' } }"
                                 :trailing-icon="appConfig.ui.icons.arrowRight"
                                 color="neutral"
                                 variant="link"
                                 size="sm"
                                 class="w-fit lg:ms-auto" />
                    </UPageCard>
                    <OverviewTimelineSection :upcoming="overview?.upcoming ?? []" />
                    <OverviewCoverageCalendar />
                </section>
                <section class="flex flex-col gap-4">
                    <UPageCard title="Runs"
                               variant="naked"
                               orientation="horizontal" />
                    <div class="grid grid-cols-1 gap-4 lg:grid-cols-2">
                        <OverviewUpcomingList :items="overview?.upcoming ?? null" />
                        <OverviewRecentList :runs="overview?.recent ?? null" />
                    </div>
                </section>
                <section class="flex flex-col gap-4">
                    <UPageCard title="Collection"
                               variant="naked"
                               orientation="horizontal" />
                    <OverviewComponentsInventory :rows="overview?.components ?? null" />
                </section>
            </template>
        </template>
    </UDashboardPanel>
</template>
