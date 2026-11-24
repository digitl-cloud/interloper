<script setup lang="ts">
/** The organisation's runs, as a table, or on a wall-clock timeline (`?view=timeline`). */
const { view, tabs } = usePageView(useRunViews())
</script>

<template>
    <UDashboardPanel id="runs"
                     :class="view === 'timeline' ? undefined : 'page-column'">
        <template #header>
            <AppNavbar title="Runs" />
            <UDashboardToolbar>
                <template #left>
                    <PageTabs :items="tabs" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <ExecutionsRunsTimeline v-if="view === 'timeline'" />
            <ExecutionsRunsTable v-else />
        </template>
    </UDashboardPanel>
</template>
